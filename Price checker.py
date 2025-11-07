# -*- coding: utf-8 -*-
import requests
import sys
import io
import threading
import psycopg2
import time
import datetime
import tkinter as tk
from tkinter import ttk, scrolledtext
import json, os
import socket
import uuid
import subprocess


def get_local_version():
    """Читает локальный version.txt, создаёт при отсутствии."""
    try:
        if not os.path.exists("version.txt"):
            with open("version.txt", "w", encoding="utf-8") as f:
                f.write("0")
            return "0"
        with open("version.txt", "r", encoding="utf-8-sig") as f:
            return f.read().strip()
    except Exception as e:
        log(f"⚠ Ошибка чтения version.txt: {e}")
        return "0"


def get_remote_version(max_retries=3, delay=3):
    """Получает актуальную версию с GitHub с анти-кэшом."""
    url = "https://raw.githubusercontent.com/wolfsum/POE/master/version.txt"
    headers = {
        "User-Agent": "PoE-AutoCollector/1.0",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=headers, params={'_': int(time.time())}, timeout=10)
            if r.status_code == 200:
                # убираем BOM и мусор
                return r.text.replace("\ufeff", "").strip()
            else:
                log(f"⚠ Ошибка при получении версии (код {r.status_code})")
        except Exception as e:
            log(f"⚠ Попытка {attempt}/{max_retries}: {e}")
            time.sleep(delay)
    return None



def update_local_version(new_version):
    """Обновляет локальный файл версии."""
    try:
        with open("version.txt", "w", encoding="utf-8") as f:
            f.write(str(new_version).strip())
        log(f"💾 Локальная версия обновлена → {new_version}")
    except Exception as e:
        log(f"⚠ Ошибка записи version.txt: {e}")


def update_from_github():
    """Скачивает свежий код и перезапускает приложение через новый процесс."""
    try:
        code_url = "https://raw.githubusercontent.com/wolfsum/POE/master/Price%20checker.py"
        version_url = "https://raw.githubusercontent.com/wolfsum/POE/master/version.txt"
        headers = {
            "User-Agent": "PoE-AutoCollector/1.0",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
        # анти-кэш
        ts = int(time.time())
        r_code = requests.get(code_url, headers=headers, params={'_': ts}, timeout=15)
        r_ver  = requests.get(version_url, headers=headers, params={'_': ts}, timeout=10)

        if r_code.status_code != 200:
            log(f"❌ Ошибка скачивания кода: {r_code.status_code}")
            return

        new_code = r_code.text
        app_file = os.path.abspath(__file__)

        try:
            with open(app_file, "r", encoding="utf-8") as f:
                old_code = f.read()
        except Exception:
            old_code = ""

        if new_code.strip() == old_code.strip():
            log("🔸 Код совпадает — обновляем только версию.")
            if r_ver.status_code == 200:
                update_local_version(r_ver.text)
            return

        # Пишем новый код в этот же файл
        with open(app_file, "w", encoding="utf-8") as f:
            f.write(new_code)
        log("✅ Код обновлён.")

        # Обновляем версию
        if r_ver.status_code == 200:
            update_local_version(r_ver.text)

        log("♻ Перезапуск программы...")
        # Стартуем новый процесс с теми же аргументами
        python = sys.executable
        args = [python] + sys.argv
        subprocess.Popen(args, close_fds=True)
        # Мгновенно выходим из текущего процесса (важно для Tkinter)
        os._exit(0)

    except Exception as e:
        log(f"❌ Ошибка обновления из GitHub: {e}")



def check_version_and_update():
    """Проверяет версию, с advisory-lock в БД, чтобы обновлял только один воркер."""
    local_ver = get_local_version()
    remote_ver = get_remote_version()

    if not remote_ver:
        log("⚠ Не удалось получить удалённую версию (GitHub недоступен).")
        return

    if remote_ver.strip() == local_ver.strip():
        log(f"🔹 Версия актуальна ({local_ver})")
        return

    # пробуем взять lock в БД: только один воркер реально обновляет
    lock_key = 777001  # любое устойчивое число
    got_lock = False
    conn = None
    try:
        conn = psycopg2.connect(**DB)
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s);", (lock_key,))
        got_lock = cur.fetchone()[0]
        conn.commit()
    except Exception as e:
        log(f"⚠ Не удалось взять advisory lock: {e}")
    finally:
        if conn:
            conn.close()

    if not got_lock:
        log(f"⌛ Обновление выполняет другой воркер. Ждём 10 сек...")
        time.sleep(10)
        return

    try:
        log(f"🆕 Найдена новая версия {remote_ver} (локально {local_ver}). Обновляем...")
        update_from_github()
    finally:
        # Снять лок (если вдруг не перезапустились)
        try:
            conn = psycopg2.connect(**DB)
            cur = conn.cursor()
            cur.execute("SELECT pg_advisory_unlock(%s);", (lock_key,))
            conn.commit()
            conn.close()
        except Exception:
            pass




def generate_worker_id():
    hostname = socket.gethostname()
    uid = str(uuid.uuid4())[:8]
    return f"{hostname}-{uid}"

def get_or_create_worker_id():
    """Получает уникальный ID воркера (сохраняется между перезапусками)."""
    if os.path.exists(CONFIG_FILE):
        try:
            data = json.load(open(CONFIG_FILE, encoding="utf-8"))
            if "worker_id" in data:
                return data["worker_id"]
        except Exception:
            pass
    worker_id = generate_worker_id()
    json.dump({"worker_id": worker_id}, open(CONFIG_FILE, "w", encoding="utf-8"))
    return worker_id

def register_worker(worker_id):
    """Регистрирует воркера в таблице collectors_status"""
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO collectors_status (worker_id, last_seen, active)
        VALUES (%s, NOW(), TRUE)
        ON CONFLICT (worker_id)
        DO UPDATE SET last_seen = NOW(), active = TRUE;
    """, (worker_id,))
    conn.commit()
    conn.close()

def update_heartbeat(worker_id):
    """Обновляет отметку активности воркера"""
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("UPDATE collectors_status SET last_seen = NOW() WHERE worker_id = %s;", (worker_id,))
    conn.commit()
    conn.close()

def start_heartbeat_thread(worker_id, interval=30):
    """Постоянно обновляет last_seen независимо от цикла."""
    def heartbeat_loop():
        while auto_running:
            try:
                conn = psycopg2.connect(**DB)
                cur = conn.cursor()
                cur.execute("UPDATE collectors_status SET last_seen = NOW() WHERE worker_id = %s;", (worker_id,))
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"[Heartbeat] Ошибка: {e}")
            time.sleep(interval)
    t = threading.Thread(target=heartbeat_loop, daemon=True)
    t.start()


def assign_group(worker_id):
    """Назначает свободную или застрявшую группу воркеру"""
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("""
        WITH next_group AS (
            SELECT id
            FROM task_groups
            WHERE completed = FALSE
              AND (
                  assigned_worker IS NULL
                  OR assigned_at < NOW() - INTERVAL '3 minutes'
                  OR assigned_worker IN (
                      SELECT worker_id FROM collectors_status
                      WHERE active = FALSE
                         OR last_seen < NOW() - INTERVAL '2 minutes'
                  )
              )
            ORDER BY id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE task_groups
        SET assigned_worker = %s,
            assigned_at = NOW()
        WHERE id IN (SELECT id FROM next_group)
        RETURNING id, range_start, range_end;
    """, (worker_id,))
    row = cur.fetchone()
    conn.commit()
    conn.close()
    return row


def mark_group_done(group_id):
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("""
        UPDATE task_groups
        SET completed = TRUE, completed_at = NOW()
        WHERE id = %s;
    """, (group_id,))
    conn.commit()
    conn.close()


def ensure_db_columns():
    """Гарантирует наличие служебных полей и индексов для кластерной работы."""
    try:
        conn = psycopg2.connect(**DB)
        cur = conn.cursor()

        # collectors_status.restarting — флаг «кто сейчас делает сброс»
        cur.execute("""
            ALTER TABLE collectors_status
            ADD COLUMN IF NOT EXISTS restarting BOOLEAN NOT NULL DEFAULT FALSE;
        """)

        # task_groups.retry_count — счётчик попыток перераспределения «зависших» групп
        cur.execute("""
            ALTER TABLE task_groups
            ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;
        """)

        # На всякий — индексы, чтобы выборки шли шустрее
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relkind = 'i' AND c.relname = 'idx_collectors_status_active_lastseen'
                ) THEN
                    CREATE INDEX idx_collectors_status_active_lastseen
                    ON collectors_status (active, last_seen);
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relkind = 'i' AND c.relname = 'idx_task_groups_completed_assigned'
                ) THEN
                    CREATE INDEX idx_task_groups_completed_assigned
                    ON task_groups (completed, assigned_worker, assigned_at);
                END IF;
            END
            $$;
        """)

        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        # если есть твой логгер — пишем туда
        try:
            log(f"⚠ ensure_db_columns: {e}")
        except Exception:
            print(f"[ensure_db_columns] {e}")
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass



CONFIG_FILE = "collector_state.json"

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ------------------ НАСТРОЙКИ ------------------
TRADE_API = "https://www.pathofexile.com/api/trade"
HEADERS = {
    "User-Agent": "PoE-Price-Collector/1.0 (+https://pathofexile.com)",
    "Accept": "application/json",
    "Content-Type": "application/json",
}
DB = dict(
    dbname="poe",
    user="postgres",
    password="Bav285111",
    host="185.103.253.157",
    port=5432,
)
DEFAULT_LEAGUE = "Keepers"

# фиксированная безопасная задержка между запросами
REQUEST_DELAY_SECONDS = 6


# ------------------ УТИЛИТЫ ------------------
def now_time():
    """Возвращает текущее время в формате ч:м:с"""
    return datetime.datetime.now().strftime("%H:%M:%S")

def load_state():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_state(state):
    try:
        existing = {}
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)
        # сохраняем старые значения, если их нет
        for key in ("worker_id", "autostart"):
            if key in existing and key not in state:
                state[key] = existing[key]

        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log(f"Ошибка сохранения состояния: {e}")





def log(msg: str):
    """Вывод с отметкой времени"""
    output_box.insert(tk.END, f"[{now_time()}] {msg}\n")
    output_box.see(tk.END)
    output_box.update()


# ------------------ API ------------------
def get_delay_from_headers(headers):
    retry_after = headers.get("Retry-After")
    if retry_after:
        try:
            return float(retry_after)
        except ValueError:
            pass
    return None


def safe_request(method, url, **kwargs):
    """Безопасный запрос с фиксированной задержкой"""
    while True:
        r = requests.request(method, url, **kwargs)
        if r.status_code == 429:
            wait_time = get_delay_from_headers(r.headers) or 60
            log(f"[RateLimit] Превышен лимит, ждём {wait_time:.1f} сек...")
            time.sleep(wait_time)
            continue
        r.raise_for_status()
        time.sleep(REQUEST_DELAY_SECONDS)
        return r


def get_leagues_list():
    """Получает список активных лиг без SSF через официальное API"""
    try:
        url = "https://api.pathofexile.com/leagues?type=main&realm=pc"
        r = safe_request("GET", url, headers=HEADERS, timeout=10)
        leagues = r.json()
        result = []
        for l in leagues:
            lid = l.get("id", "")
            # исключаем SSF, Ruthless и временные event-лиги
            if any(x in lid for x in ("SSF", "Ruthless", "Event")):
                continue
            result.append(lid)
        if not result:
            result = [DEFAULT_LEAGUE, "Standard", "Hardcore"]
        log(f"Загружено {len(result)} лиг: {', '.join(result)}")
        return result
    except Exception as e:
        log(f"Ошибка загрузки списка лиг: {e}")
        return [DEFAULT_LEAGUE, "Standard", "Hardcore"]



def get_item_types_from_db():
    try:
        conn = psycopg2.connect(**DB)
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ui.item_type
            FROM unique_items ui
            WHERE ui.item_type IS NOT NULL
            ORDER BY ui.item_type;
        """)
        rows = [r[0] for r in cur.fetchall()]
        conn.close()
        return ["Все"] + rows if rows else ["Все"]
    except Exception as e:
        log(f"Ошибка загрузки типов из БД: {e}")
        return ["Все"]


def search_items(name, base, league="Keepers", limit=1, status="securable",
                 corrupted_choice="да", stat_id=None, session_id=None):
    """Поиск предметов через PoE Trade API"""
    query = {
        "status": {"option": status},
        "name": name,
        "type": base,
        "stats": [{"type": "and", "filters": []}],
    }

    query.setdefault("filters", {"misc_filters": {"filters": {}}})
    if corrupted_choice.lower() == "да":
        query["filters"]["misc_filters"]["filters"]["corrupted"] = {"option": True}
    elif corrupted_choice.lower() == "нет":
        query["filters"]["misc_filters"]["filters"]["corrupted"] = {"option": False}

    if stat_id:
        query["stats"][0]["filters"].append({
            "id": stat_id,
            "value": {},
            "disabled": False
        })

    payload = {"query": query, "sort": {"price": "asc"}}
    headers = HEADERS.copy()
    cookies = {"POESESSID": session_id} if session_id else {}

    log(f"Запрос → {name} ({base})")
    start_time = time.time()
    r = safe_request("POST", f"{TRADE_API}/search/{league}",
                     headers=headers, cookies=cookies, json=payload, timeout=15)
    data = r.json()
    ids = data.get("result", [])
    elapsed = time.time() - start_time
    log(f"Ответ ({len(ids)} id) за {elapsed:.2f} сек")

    if not ids:
        return []

    results = []
    for i in range(0, min(limit, len(ids)), 10):
        chunk = ids[i:i + 10]
        fetch_url = f"{TRADE_API}/fetch/{','.join(chunk)}?query={data['id']}"
        log(f"  Fetch {i+1}-{i+len(chunk)}")
        start_chunk = time.time()
        r2 = safe_request("GET", fetch_url, headers=headers, cookies=cookies, timeout=10)
        results.extend(r2.json().get("result", []))
        log(f"  Получено {len(results)} результатов (+{len(chunk)}) за {time.time()-start_chunk:.2f} сек")
    return results


def parse_price_entry(entry):
    listing = entry.get("listing", {})
    price = listing.get("price", {})
    acc = listing.get("account", {})
    if not price:
        return None, None, None
    return price.get("amount"), price.get("currency"), acc.get("name")


# ------------------ БД ------------------
def get_next_row_after(last_id, item_type_filter=None):
    """Берём следующую запись после last_id; если конец — начинаем заново"""
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    base_query = """
        SELECT 
            i.id, i.item_name, i.base_type, i.mod_description,
            i.stat_id, ui.item_type
        FROM trade_prices AS i
        LEFT JOIN unique_items AS ui ON ui.name = i.item_name
    """
    where = []
    params = []
    if last_id:
        where.append("i.id > %s")
        params.append(last_id)
    if item_type_filter and item_type_filter.lower() != "все":
        where.append("ui.item_type = %s")
        params.append(item_type_filter)
    if where:
        base_query += " WHERE " + " AND ".join(where)
    base_query += " ORDER BY i.id LIMIT 1;"
    try:
        cur.execute(base_query, tuple(params))
        row = cur.fetchone()
        if not row:
            cur.execute("""
                SELECT 
                    i.id, i.item_name, i.base_type, i.mod_description,
                    i.stat_id, ui.item_type
                FROM trade_prices AS i
                LEFT JOIN unique_items AS ui ON ui.name = i.item_name
                ORDER BY i.id
                LIMIT 1;
            """)
            row = cur.fetchone()
        return row
    finally:
        conn.close()


def update_price_in_db(row_id, value, currency, seller, league=DEFAULT_LEAGUE):
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("""
        UPDATE trade_prices
        SET price_value = %s,
            currency_id = %s,
            seller_name = %s,
            league = %s,
            updated_at = NOW()
        WHERE id = %s;
    """, (value, currency, seller, league, row_id))
    conn.commit()
    conn.close()


def deactivate_stale_workers():
    """Деактивирует воркеров, не подававших сигнал более 2 минут."""
    try:
        conn = psycopg2.connect(**DB)
        cur = conn.cursor()
        cur.execute("""
            UPDATE collectors_status
            SET active = FALSE
            WHERE last_seen < NOW() - INTERVAL '2 minutes'
              AND active = TRUE;
        """)
        affected = cur.rowcount
        conn.commit()
        conn.close()
        if affected > 0:
            log(f"⚠ Деактивировано воркеров: {affected}")
    except Exception as e:
        log(f"Ошибка при деактивации неактивных воркеров: {e}")


# ------------------ GUI ------------------
root = tk.Tk()
root.title("PoE Auto Price Collector (Cluster Edition)")
root.geometry("1100x800")

frame_top = ttk.Frame(root, padding=5)
frame_top.pack(fill="x")

# Лига
ttk.Label(frame_top, text="Лига:").grid(row=0, column=0, sticky="w")
league_cb = ttk.Combobox(frame_top, width=20, state="readonly",
                         values=[DEFAULT_LEAGUE, "Mercenaries", "Hardcore", "Standard"])
league_cb.grid(row=0, column=1, padx=5)
league_cb.set(DEFAULT_LEAGUE)

# Статус продавца
ttk.Label(frame_top, text="Статус продавца:").grid(row=0, column=2, sticky="w")
status_cb = ttk.Combobox(frame_top, width=18, state="readonly",
                         values=["securable", "onlineleague", "any"])
status_cb.grid(row=0, column=3, padx=5)
status_cb.set("securable")

# POESESSID (необязателен)
ttk.Label(frame_top, text="POESESSID (опционально):").grid(row=0, column=4, sticky="w")
session_entry = ttk.Entry(frame_top, width=40)
session_entry.grid(row=0, column=5, padx=5)

# Кнопки
btn_start = ttk.Button(frame_top, text="▶ Запустить автопоиск")
btn_start.grid(row=0, column=6, padx=10)

btn_stop = ttk.Button(frame_top, text="⛔ Остановить", state="disabled")
btn_stop.grid(row=0, column=7, padx=5)

# Окно вывода логов
output_box = scrolledtext.ScrolledText(root, wrap=tk.WORD, font=("Consolas", 10))
output_box.pack(fill="both", expand=True, padx=5, pady=5)





# ------------------ Загрузка состояния ------------------
state = load_state()
if state:
    if "poesessid" in state and state["poesessid"]:
        session_entry.insert(0, state["poesessid"])
        log("POESESSID восстановлен из состояния.")
    if "league" in state:
        league_cb.set(state["league"])
    if "status" in state:
        status_cb.set(state["status"])


# ------------------ ЛОГИКА ------------------
auto_running = False


def auto_loop():
    global auto_running

    worker_id = get_or_create_worker_id()
    register_worker(worker_id)
    ensure_db_columns() 
    log(f"✅ Воркер зарегистрирован: {worker_id}")

    league = league_cb.get().strip()
    status = status_cb.get().strip()
    session_id = session_entry.get().strip() or None

    # сохраняем состояние
    save_state({
        "poesessid": session_entry.get().strip(),
        "league": league_cb.get().strip(),
        "status": status_cb.get().strip()
    })

    # --- проверка состояния групп ---
    try:
        conn = psycopg2.connect(**DB)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM task_groups WHERE completed = FALSE;")
        pending = cur.fetchone()[0]

        if pending == 0:
            log("♻ Все группы завершены — начинается новый цикл сбора...")
            cur.execute("""
                UPDATE task_groups
                SET completed = FALSE,
                    assigned_worker = NULL,
                    assigned_at = NULL,
                    completed_at = NULL;
            """)
            conn.commit()
            log("✅ Группы сброшены для нового цикла.")
        else:
            log(f"ℹ Осталось незавершённых групп: {pending}")
        conn.close()
    except Exception as e:
        log(f"Ошибка при проверке состояния групп: {e}")

    # --- сервисные функции ---
    def deactivate_stale_workers():
        """Деактивирует воркеров, не подававших сигнал более 2 минут."""
        try:
            conn = psycopg2.connect(**DB)
            cur = conn.cursor()
            cur.execute("""
                UPDATE collectors_status
                SET active = FALSE
                WHERE last_seen < NOW() - INTERVAL '2 minutes'
                  AND active = TRUE;
            """)
            affected = cur.rowcount
            conn.commit()
            conn.close()
            if affected > 0:
                log(f"⚠ Деактивировано воркеров: {affected}")
        except Exception as e:
            log(f"Ошибка при деактивации неактивных воркеров: {e}")

    def release_stale_groups():
        """Освобождает зависшие группы, если воркер не активен более 2 минут."""
        try:
            conn = psycopg2.connect(**DB)
            cur = conn.cursor()
            cur.execute("""
                UPDATE task_groups
                SET assigned_worker = NULL,
                    assigned_at = NULL,
                    retry_count = retry_count + 1
                WHERE completed = FALSE
                  AND assigned_worker IN (
                      SELECT worker_id FROM collectors_status
                      WHERE active = FALSE
                         OR last_seen < NOW() - INTERVAL '2 minutes'
                  );
            """)
            released = cur.rowcount
            conn.commit()
            conn.close()
            if released > 0:
                log(f"⚠ Освобождено зависших групп: {released}")
        except Exception as e:
            log(f"Ошибка при проверке зависших групп: {e}")

    # --- основной цикл ---
    last_recheck = 0
    last_heartbeat = 0

    log(f"▶ Автопоиск запущен (лига: {league}, статус: {status})")

    while auto_running:
        try:
            now = time.time()

            # каждые 2 минуты чистим неактивных воркеров и зависшие группы
            if now - last_recheck > 120:
                deactivate_stale_workers()
                release_stale_groups()
                last_recheck = now

            # --- получение свободной группы ---
            group = assign_group(worker_id)
            if not group:
                # Проверим, все ли группы завершены
                try:
                    conn = psycopg2.connect(**DB)
                    cur = conn.cursor()
                    cur.execute("SELECT COUNT(*) FROM task_groups WHERE completed = FALSE;")
                    pending = cur.fetchone()[0]
            
                    if pending == 0:
                        # Проверим, не делает ли кто-то другой сброс
                        cur.execute("SELECT COUNT(*) FROM collectors_status WHERE restarting = TRUE;")
                        already_restarting = cur.fetchone()[0]
            
                        if already_restarting == 0:
                            # Берем право на сброс
                            cur.execute("""
                                UPDATE collectors_status
                                SET restarting = TRUE
                                WHERE worker_id = %s
                                AND NOT EXISTS (
                                    SELECT 1 FROM collectors_status WHERE restarting = TRUE
                                )
                                RETURNING worker_id;
                            """, (worker_id,))
                            res = cur.fetchone()
                            if res:
                                log("♻ Все группы завершены — этот воркер инициирует сброс...")
                                cur.execute("""
                                    UPDATE task_groups
                                    SET completed = FALSE,
                                        assigned_worker = NULL,
                                        assigned_at = NULL,
                                        completed_at = NULL;
                                """)
                                conn.commit()
                                log("✅ Группы успешно сброшены. Новый цикл начат.")
                            else:
                                log("⌛ Сброс выполняется другим воркером, ожидаем 10 сек...")
                                time.sleep(10)
                        else:
                            log("⌛ Обнаружен сброс — ждём 10 сек...")
                            time.sleep(10)
            
                        # После сброса сбрасываем флаг у всех
                        cur.execute("UPDATE collectors_status SET restarting = FALSE;")
                        conn.commit()
                        conn.close()
                        continue
                    else:
                        conn.close()
                        log("⏸ Нет свободных групп — ожидание 60 сек...")
                        time.sleep(60)
                        continue
            
                except Exception as e:
                    log(f"Ошибка при проверке завершения групп: {e}")
                    time.sleep(30)
                    continue


            group_id, start_id, end_id = group
            log(f"📦 Получена группа {group_id}: ID {start_id}-{end_id}")

            # фиксируем назначение в collectors_status
            try:
                conn = psycopg2.connect(**DB)
                cur = conn.cursor()
                cur.execute("""
                    UPDATE collectors_status
                    SET current_group = %s,
                        last_group_update = NOW()
                    WHERE worker_id = %s;
                """, (group_id, worker_id))
                conn.commit()
                conn.close()
            except Exception as e:
                log(f"Ошибка записи текущей группы в collectors_status: {e}")

            # --- обработка диапазона ---
            conn = psycopg2.connect(**DB)
            cur = conn.cursor()
            cur.execute("""
                SELECT 
                    i.id, i.item_name, i.base_type, i.mod_description,
                    i.stat_id, ui.item_type
                FROM trade_prices AS i
                LEFT JOIN unique_items AS ui ON ui.name = i.item_name
                WHERE i.id BETWEEN %s AND %s
                ORDER BY i.id;
            """, (start_id, end_id))
            rows = cur.fetchall()
            conn.close()

            for row_id, name, base, mod, stat_id, item_type in rows:
                if not auto_running:
                    break
                log(f"→ {row_id}: {name} ({base}), тип: {item_type}, мод: {mod}")

                results = search_items(name, base, league, 1, status, "да", stat_id, session_id)
                if not results:
                    update_price_in_db(row_id, None, None, None, league)
                    log("   Не найдено")
                else:
                    value, currency, seller = parse_price_entry(results[0])
                    update_price_in_db(row_id, value, currency, seller, league)
                    log(f"   {value} {currency} (продавец: {seller})")

            # --- завершение группы ---
            # --- завершение группы ---
            mark_group_done(group_id)
            log(f"✅ Группа {group_id} завершена")
            check_version_and_update()  # <-- сюда


        except Exception as e:
            log(f"Ошибка: {e}")
            time.sleep(5)

        try:
            root.update()
        except Exception:
            pass

    log("⛔ Автопоиск остановлен")




def stop_auto_search():
    """Остановка автопоиска и деактивация воркера."""
    global auto_running
    if not auto_running:
        log("⚠ Автопоиск уже остановлен.")
        return

    auto_running = False
    btn_start.config(state="normal")
    btn_stop.config(state="disabled")

    # сбрасываем флаг автозапуска
    st = load_state()
    st["autostart"] = False
    save_state(st)

    # помечаем воркера как неактивного
    try:
        worker_id = get_or_create_worker_id()
        conn = psycopg2.connect(**DB)
        cur = conn.cursor()
        cur.execute("""
            UPDATE collectors_status
            SET active = FALSE, last_seen = NOW()
            WHERE worker_id = %s;
        """, (worker_id,))
        conn.commit()
        conn.close()
        log(f"⛔ Воркер {worker_id} остановлен.")
    except Exception as e:
        log(f"Ошибка при остановке воркера: {e}")

    log("⛔ Автопоиск остановлен.")



def on_close():
    stop_auto_search()
    root.destroy()

root.protocol("WM_DELETE_WINDOW", on_close)


def start_auto_search():
    """Запуск автопоиска и сохранение состояния."""
    global auto_running, worker_id
    if auto_running:
        log("⚠ Автопоиск уже запущен.")
        return

    if 'worker_id' not in globals():
        worker_id = get_or_create_worker_id()

    auto_running = True
    btn_start.config(state="disabled")
    btn_stop.config(state="normal")

    # сохраняем, что автопоиск активен (для автозапуска при рестарте)
    st = load_state()
    st["autostart"] = True
    st["poesessid"] = session_entry.get().strip()
    st["league"] = league_cb.get().strip()
    st["status"] = status_cb.get().strip()
    save_state(st)

    # запускаем обновление last_seen в фоне
    start_heartbeat_thread(worker_id, interval=30)

    # запускаем основной цикл
    threading.Thread(target=auto_loop, daemon=True).start()
    log(f"▶ Запущен автопоиск для воркера: {worker_id}")





btn_start.config(command=start_auto_search)
btn_stop.config(command=stop_auto_search)

# ------------------ MAIN ------------------
log("PoE Auto Price Collector готов к работе.")

# 🔹 Проверяем версию при запуске (мягко, без блокировки)
try:
    check_version_and_update()
except Exception as e:
    log(f"⚠ Стартовая проверка версии: {e}")

# 🔹 Автостарт после рестарта, если autostart = True
try:
    st = load_state()
    if st.get("autostart"):
        if st.get("poesessid"):
            session_entry.delete(0, tk.END)
            session_entry.insert(0, st["poesessid"])
        if st.get("league"):
            league_cb.set(st["league"])
        if st.get("status"):
            status_cb.set(st["status"])
        start_auto_search()
        log("⚙ Автостарт включен — автопоиск запущен после рестарта.")
except Exception as e:
    log(f"⚠ Ошибка автозапуска: {e}")

# 🔹 Запуск GUI
root.mainloop()