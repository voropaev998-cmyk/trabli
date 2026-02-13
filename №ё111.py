import os
import sys
import time
import json
import re
import csv
import logging
import logging.handlers
import requests
import base64
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import urljoin
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from collections import defaultdict

# Для Google Sheets
try:
    import gspread
    from google.oauth2.service_account import Credentials

    GOOGLE_SHEETS_AVAILABLE = True
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False
    print("Предупреждение: Библиотеки для Google Sheets не установлены. Установите: pip install gspread google-auth")
    print("Данные будут сохраняться только в локальные файлы.")


# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
def setup_logging(log_level=logging.INFO):
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_format = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.handlers.clear()

    file_handler = logging.handlers.RotatingFileHandler(
        filename=log_dir / 'element_monitor.log',
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(logging.Formatter(log_format, date_format))

    error_handler = logging.handlers.RotatingFileHandler(
        filename=log_dir / 'errors.log',
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.WARNING)
    error_handler.setFormatter(logging.Formatter(log_format, date_format))

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', date_format)
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)

    telegram_logger = logging.getLogger('telegram')
    telegram_logger.setLevel(logging.DEBUG)
    telegram_logger.propagate = False
    telegram_handler = logging.handlers.RotatingFileHandler(
        filename=log_dir / 'telegram_debug.log',
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding='utf-8'
    )
    telegram_handler.setLevel(logging.DEBUG)
    telegram_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    telegram_logger.addHandler(telegram_handler)

    selenium_logger = logging.getLogger('selenium')
    selenium_logger.setLevel(logging.WARNING)
    selenium_logger.propagate = False
    selenium_handler = logging.handlers.RotatingFileHandler(
        filename=log_dir / 'selenium.log',
        maxBytes=2 * 1024 * 1024,
        backupCount=2,
        encoding='utf-8'
    )
    selenium_handler.setLevel(logging.WARNING)
    selenium_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    selenium_logger.addHandler(selenium_handler)

    logger.info(f"Логирование инициализировано. Уровень: {logging.getLevelName(log_level)}")
    return logger


logger = setup_logging(logging.INFO)


# ==================== TELEGRAM BOT ====================
class TelegramBot:
    def __init__(self, token):
        self.token = token
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.telegram_logger = logging.getLogger('telegram')

        if not token:
            logger.warning("Telegram токен не настроен. Отправка в Telegram отключена.")
            self.enabled = False
        else:
            self.enabled = True
            logger.info(f"Telegram бот инициализирован. Токен: {token[:5]}...")

    def send_message_to_chat(self, chat_id, text, parse_mode='HTML'):
        if not self.enabled:
            return False
        if not chat_id:
            logger.warning("Не указан chat_id, пропускаем отправку")
            return False
        try:
            url = f"{self.base_url}/sendMessage"
            payload = {
                'chat_id': chat_id,
                'text': text,
                'parse_mode': parse_mode,
                'disable_web_page_preview': True
            }
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                logger.info(f"✅ Сообщение отправлено в чат {chat_id}")
                self.telegram_logger.info(f"Сообщение отправлено в {chat_id}: {text[:100]}...")
                return True
            else:
                logger.error(f"❌ Ошибка отправки в чат {chat_id}: {response.status_code} - {response.text}")
                self.telegram_logger.error(f"Ошибка {response.status_code} для чата {chat_id}: {response.text}")
                return False
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке в чат {chat_id}: {e}")
            self.telegram_logger.error(f"Исключение для чата {chat_id}: {e}")
            return False

    def send_photo_bytes_to_chat(self, chat_id, photo_data, caption="", parse_mode='HTML'):
        if not self.enabled:
            return False
        if not chat_id:
            logger.warning("Не указан chat_id, пропускаем отправку фото")
            return False
        try:
            url = f"{self.base_url}/sendPhoto"
            files = {'photo': ('photo.jpg', photo_data, 'image/jpeg')}
            data = {
                'chat_id': chat_id,
                'caption': caption[:1024] if caption else "",
                'parse_mode': parse_mode
            }
            response = requests.post(url, files=files, data=data, timeout=15)
            if response.status_code == 200:
                logger.info(f"✅ Фото отправлено в чат {chat_id}")
                self.telegram_logger.info(f"Фото отправлено в {chat_id}")
                return True
            else:
                logger.error(f"❌ Ошибка отправки фото в чат {chat_id}: {response.status_code} - {response.text}")
                self.telegram_logger.error(f"Ошибка фото {response.status_code} для чата {chat_id}: {response.text}")
                return False
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке фото в чат {chat_id}: {e}")
            self.telegram_logger.error(f"Исключение для чата {chat_id}: {e}")
            return False

    def send_media_group_bytes_to_chat(self, chat_id, photos_data, caption=""):
        if not self.enabled:
            return False
        if not chat_id:
            logger.warning("Не указан chat_id, пропускаем отправку медиагруппы")
            return False
        if not photos_data:
            return False
        try:
            photos_data = photos_data[:10]
            media = []
            files = {}
            for i, photo_data in enumerate(photos_data):
                if not photo_data or len(photo_data) < 1024:
                    continue
                file_name = f"photo_{i}"
                media_item = {'type': 'photo', 'media': f'attach://{file_name}'}
                if i == 0 and caption:
                    media_item['caption'] = caption[:1024]
                    media_item['parse_mode'] = 'HTML'
                media.append(media_item)
                files[file_name] = (f'photo_{i}.jpg', photo_data, 'image/jpeg')
            if not media:
                return False
            files['media'] = (None, json.dumps(media), 'application/json')
            url = f"{self.base_url}/sendMediaGroup"
            params = {'chat_id': chat_id}
            response = requests.post(url, params=params, files=files, timeout=30)
            if response.status_code == 200:
                logger.info(f"✅ Медиагруппа из {len(media)} фото отправлена в чат {chat_id}")
                self.telegram_logger.info(f"Медиагруппа в {chat_id}: {len(media)} фото")
                return True
            else:
                logger.error(f"❌ Ошибка медиагруппы в чат {chat_id}: {response.status_code} - {response.text}")
                self.telegram_logger.error(f"Ошибка медиагруппы для чата {chat_id}: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"❌ Ошибка медиагруппы в чат {chat_id}: {e}")
            self.telegram_logger.error(f"Исключение для чата {chat_id}: {e}")
            return False


# ==================== GOOGLE SHEETS ====================
class GoogleSheetManager:
    def __init__(self, credentials_path, spreadsheet_url):
        self.credentials_path = credentials_path
        self.spreadsheet_url = spreadsheet_url
        self.client = None
        self.spreadsheet = None
        self.worksheet = None
        self.lookup_worksheet = None
        self.address_district_map = {}

        self.headers = [
            "Timestamp", "ID Задания", "Адрес", "Тип тары",
            "Проблематика", "Городской округ", "ФИО", "ТС",
            "Фото (ссылки)", "Статус обработки", "Telegram отправлено"
        ]
        self.headers.append("Округ (VLOOKUP)")

        self.setup_google_sheets()

    def setup_google_sheets(self):
        if not GOOGLE_SHEETS_AVAILABLE:
            return False
        try:
            scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file']
            if not os.path.exists(self.credentials_path):
                logger.error(f"Файл учетных данных не найден: {self.credentials_path}")
                return False
            creds = Credentials.from_service_account_file(self.credentials_path, scopes=scope)
            self.client = gspread.authorize(creds)

            spreadsheet_id = self.extract_spreadsheet_id(self.spreadsheet_url)
            if not spreadsheet_id:
                return False
            self.spreadsheet = self.client.open_by_key(spreadsheet_id)

            self.worksheet = self.spreadsheet.get_worksheet(0)
            existing_headers = self.worksheet.row_values(1)
            if len(existing_headers) < len(self.headers):
                self.worksheet.update('A1', [self.headers])
                logger.info("Заголовки обновлены (добавлен столбец Округ VLOOKUP)")
            else:
                logger.info("Заголовки в Google Таблице уже существуют")

            try:
                self.lookup_worksheet = self.spreadsheet.worksheet("Лист2")
                logger.info("✅ Лист2 найден, загружаем данные для VLOOKUP...")
                self.load_lookup_data()
            except gspread.WorksheetNotFound:
                logger.warning(
                    "Лист2 не найден. Создайте лист с именем 'Лист2' и столбцами A: Адрес, B: Городской округ")
                self.lookup_worksheet = None

            logger.info("✅ Google Таблицы успешно подключены")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к Google Таблицам: {e}")
            return False

    def load_lookup_data(self):
        self.address_district_map.clear()
        if not self.lookup_worksheet:
            return
        try:
            all_rows = self.lookup_worksheet.get_all_values()
            if not all_rows:
                return
            start_row = 1 if all_rows[0][0].lower() in ['адрес', 'address'] else 0
            for row in all_rows[start_row:]:
                if len(row) >= 2 and row[0].strip():
                    address = row[0].strip().lower()
                    district = row[1].strip()
                    self.address_district_map[address] = district
            logger.info(f"Загружено {len(self.address_district_map)} записей из Лист2")
        except Exception as e:
            logger.error(f"Ошибка загрузки данных из Лист2: {e}")

    def get_district_by_address(self, address):
        if not address:
            return None
        addr_lower = address.lower().strip()
        if addr_lower in self.address_district_map:
            return self.address_district_map[addr_lower]
        for key, value in self.address_district_map.items():
            if key in addr_lower or addr_lower in key:
                return value
        return None

    def extract_spreadsheet_id(self, url):
        patterns = [r'/spreadsheets/d/([a-zA-Z0-9-_]+)', r'd/([a-zA-Z0-9-_]+)']
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return url if len(url) > 20 and '/' not in url else None

    def add_row(self, data):
        if not self.worksheet:
            return False
        try:
            address = data.get('address', '')
            district_from_lookup = self.get_district_by_address(address)
            if district_from_lookup:
                data['city_district'] = district_from_lookup
                logger.info(f"✅ Городской округ определен через VLOOKUP: {district_from_lookup}")
            else:
                logger.warning(
                    f"Адрес '{address}' не найден в Лист2, оставляем извлеченное значение: {data.get('city_district', 'Не определено')}")

            row_data = [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                data.get('task_id', 'Неизвестно'),
                address,
                data.get('container_type', ''),
                data.get('problem', ''),
                data.get('city_district', ''),
                data.get('driver_name', ''),
                data.get('vehicle', ''),
                data.get('photos_str', 'Нет фото'),
                data.get('status', 'Успешно'),
                data.get('telegram_sent', 'Нет'),
                ''
            ]

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    added_row = self.worksheet.append_row(row_data, value_input_option='USER_ENTERED')
                    logger.info(f"✅ Данные добавлены в Google Таблицу: {address[:50]}...")

                    all_values = self.worksheet.get_all_values()
                    row_number = len(all_values)

                    formula = f'=VLOOKUP(C{row_number};\'Лист2\'!A:B;2;0)'
                    self.worksheet.update_cell(row_number, 12, formula)
                    logger.debug(f"Вставлена формула в L{row_number}: {formula}")

                    return True
                except Exception as e:
                    if attempt < max_retries - 1:
                        time.sleep(2)
                    else:
                        logger.error(f"❌ Ошибка добавления данных в Google Таблицу после {max_retries} попыток: {e}")
                        return False
        except Exception as e:
            logger.error(f"❌ Ошибка добавления данных в Google Таблицу: {e}")
            return False


# ==================== CSV MANAGER ====================
class CSVManager:
    def __init__(self, filename="monitoring_data.csv"):
        self.filename = filename
        self.headers = [
            "Timestamp", "ID Задания", "Адрес", "Тип тары",
            "Проблематика", "Городской округ", "ФИО", "ТС",
            "Фото (ссылки)", "Статус обработки", "Telegram отправлено"
        ]
        self.setup_csv()

    def setup_csv(self):
        try:
            file_exists = Path(self.filename).exists()
            if not file_exists:
                with open(self.filename, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f, delimiter=';')
                    writer.writerow(self.headers)
                logger.info(f"✅ Создан CSV файл: {self.filename}")
        except Exception as e:
            logger.error(f"❌ Ошибка создания CSV файла: {e}")

    def add_row(self, data):
        try:
            row_data = [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                data.get('task_id', 'Неизвестно'),
                data.get('address', ''),
                data.get('container_type', ''),
                data.get('problem', ''),
                data.get('city_district', ''),
                data.get('driver_name', ''),
                data.get('vehicle', ''),
                data.get('photos_str', 'Нет фото'),
                data.get('status', 'Успешно'),
                data.get('telegram_sent', 'Нет')
            ]
            with open(self.filename, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerow(row_data)
            logger.info(f"✅ Данные сохранены в CSV: {data.get('address', '')[:50]}...")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в CSV: {e}")
            return False


# ==================== ELEMENT MONITOR ====================
class ElementMonitor:
    def __init__(self):
        env_path = Path(r"C:\Users\vorop\PyCharmMiscProject\.env")
        if not env_path.exists():
            raise FileNotFoundError(f"Файл .env не найден по пути: {env_path}")

        load_dotenv(dotenv_path=env_path)

        required_vars = ['SITE_USERNAME', 'SITE_PASSWORD', 'SITE_URL']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Добавьте в .env: {', '.join(missing_vars)}")

        self.config = {
            'username': os.getenv('SITE_USERNAME'),
            'password': os.getenv('SITE_PASSWORD'),
            'site_url': os.getenv('SITE_URL').rstrip('/'),
            'monitor_interval': int(os.getenv('MONITOR_INTERVAL', '5')),  # по умолчанию 5 секунд
            'save_screenshots': os.getenv('SAVE_SCREENSHOTS', 'True').lower() == 'true',
            'headless': os.getenv('HEADLESS_MODE', 'False').lower() == 'true',
            'google_credentials': os.getenv('GOOGLE_CREDENTIALS_PATH', 'credentials.json'),
            'google_sheet_url': os.getenv('GOOGLE_SHEET_URL', ''),
            'telegram_token': os.getenv('TELEGRAM_TOKEN'),
            'telegram_chat_podolsk': os.getenv('TELEGRAM_CHAT_PODOLSK', ''),
            'telegram_chat_chekhov': os.getenv('TELEGRAM_CHAT_CHEKHOV', ''),
            'telegram_chat_south': os.getenv('TELEGRAM_CHAT_SOUTH', ''),
            'save_photos_locally': os.getenv('SAVE_PHOTOS_LOCALLY', 'True').lower() == 'true',
            'send_media_group': os.getenv('SEND_MEDIA_GROUP', 'True').lower() == 'true',
            'log_level': os.getenv('LOG_LEVEL', 'INFO').upper(),
            'max_retry_attempts': int(os.getenv('MAX_RETRY_ATTEMPTS', '3')),
            'report_interval_hours': int(os.getenv('REPORT_INTERVAL_HOURS', '3'))
        }

        log_level = getattr(logging, self.config['log_level'], logging.INFO)
        logger.setLevel(log_level)

        logger.info(f"Конфигурация загружена. Сайт: {self.config['site_url']}")

        # Инициализация Telegram бота
        self.telegram_bot = TelegramBot(self.config['telegram_token'])

        # Настройка трёх чатов
        self.chat_ids = {
            'podolsk': self.config.get('telegram_chat_podolsk'),
            'chekhov': self.config.get('telegram_chat_chekhov'),
            'south': self.config.get('telegram_chat_south')
        }

        enabled_chats = [key for key, chat in self.chat_ids.items() if chat]
        if enabled_chats:
            logger.info(f"Telegram чаты настроены: {', '.join(enabled_chats)}")
        else:
            logger.warning("Ни один Telegram чат не настроен. Отправка в Telegram отключена.")
            self.telegram_bot.enabled = False

        # Словарь соответствия округов чатам
        self.district_to_chat_key = {
            'подольск': 'podolsk',
            'чехов': 'chekhov',
            'серпухов': 'south',
            'пущино': 'south',
            'протвино': 'south'
        }

        self.driver = None
        self.monitoring_active = False
        self.processed_tasks = set()
        self.failed_tasks = {}

        # Хранилище для отчётов
        self.report_stats = {
            'podolsk': defaultdict(lambda: defaultdict(int)),
            'chekhov': defaultdict(lambda: defaultdict(int)),
            'south': defaultdict(lambda: defaultdict(int))
        }
        self.last_report_time = datetime.now()

        self.google_sheets = None
        self.csv_manager = CSVManager("monitoring_data.csv")

        if GOOGLE_SHEETS_AVAILABLE and self.config['google_sheet_url']:
            self.google_sheets = GoogleSheetManager(
                self.config['google_credentials'],
                self.config['google_sheet_url']
            )

        self.task_selector = "span.stand_info.ng-binding"

        self.stats = {
            'total_checks': 0,
            'tasks_found': 0,
            'tasks_processed': 0,
            'tasks_retried': 0,
            'tasks_failed_permanent': 0,
            'errors': 0,
            'saved_to_google': 0,
            'saved_to_csv': 0,
            'sent_to_telegram': 0,
            'telegram_podolsk': 0,
            'telegram_chekhov': 0,
            'telegram_south': 0,
            'photos_found': 0,
            'photos_sent': 0,
            'photos_captured': 0,
            'photos_failed': 0,
            'media_groups_sent': 0,
            'single_photos_sent': 0,
            'vlookup_matches': 0,
            'vlookup_misses': 0,
            'reports_sent': 0
        }

        self.debug_dir = Path("debug_logs")
        self.debug_dir.mkdir(exist_ok=True)
        self.photos_dir = Path("downloaded_photos")
        self.photos_dir.mkdir(exist_ok=True)

    def setup_driver(self):
        chrome_options = Options()
        if self.config['headless']:
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_argument('--log-level=3')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--disable-notifications')
        chrome_options.add_argument('--disable-popup-blocking')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_experimental_option('prefs', {
            'profile.default_content_setting_values.notifications': 2,
            'profile.managed_default_content_settings.images': 1,
            'download.default_directory': str(self.photos_dir.absolute()),
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'safebrowsing.enabled': True
        })
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            logger.info("WebDriver успешно инициализирован")
            return True
        except Exception as e:
            logger.error(f"Ошибка инициализации WebDriver: {e}")
            return False

    def login(self):
        try:
            logger.info(f"Переходим на страницу входа: {self.config['site_url']}")
            self.driver.get(self.config['site_url'])
            wait = WebDriverWait(self.driver, 30)
            username_field = wait.until(EC.presence_of_element_located((By.ID, "j_username")))
            password_field = self.driver.find_element(By.ID, "j_password")
            login_button = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
            username_field.clear()
            username_field.send_keys(self.config['username'])
            password_field.clear()
            password_field.send_keys(self.config['password'])
            login_button.click()
            time.sleep(5)
            if "login" in self.driver.current_url.lower() or "auth" in self.driver.current_url.lower():
                logger.error("Не удалось войти. Проверьте учетные данные.")
                return False
            logger.info("✅ Успешный вход в систему")
            return True
        except Exception as e:
            logger.error(f"Ошибка при входе в систему: {e}")
            return False

    def navigate_to_monitor_page(self):
        try:
            dispatch_url = f"{self.config['site_url']}/index.html#/dispatcher/dispatch"
            self.driver.get(dispatch_url)
            time.sleep(5)
            self.switch_to_routes_tab()
            time.sleep(5)
            logger.info("✅ Перешли на страницу мониторинга")
            return True
        except Exception as e:
            logger.error(f"Ошибка при переходе к странице мониторинга: {e}")
            return False

    def switch_to_routes_tab(self):
        try:
            routes_selectors = [
                'label[uib-btn-radio="\'ROUTES\'"]',
                '//label[contains(text(), "Маршруты")]',
                '//button[contains(text(), "Маршруты")]',
                '//a[contains(text(), "Маршруты")]'
            ]
            for selector in routes_selectors:
                try:
                    routes_tab = self.driver.find_element(By.XPATH, selector) if selector.startswith(
                        '//') else self.driver.find_element(By.CSS_SELECTOR, selector)
                    logger.info("Найдена вкладка 'Маршруты'")
                    classes = routes_tab.get_attribute('class')
                    if 'active' not in classes and 'btn-primary' not in classes and 'selected' not in classes:
                        routes_tab.click()
                        logger.info("Активирована вкладка 'Маршруты'")
                        time.sleep(3)
                    else:
                        logger.info("Вкладка 'Маршруты' уже активна")
                    return True
                except:
                    continue
            logger.warning("Вкладка 'Маршруты' не найдена")
            return False
        except Exception as e:
            logger.error(f"Ошибка переключения на вкладку Маршруты: {e}")
            return False

    def find_all_tasks(self):
        try:
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, self.task_selector)))
            except TimeoutException:
                alternative_selectors = ["span[ng-click*='openRouteTaskInfo']", ".stand_info", ".ng-binding[ng-click]"]
                for selector in alternative_selectors:
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if elements:
                            logger.info(f"Найдено {len(elements)} заданий по селектору: {selector}")
                            tasks = elements
                            break
                    except:
                        continue
                else:
                    logger.warning("Задания не найдены")
                    return []
            tasks = self.driver.find_elements(By.CSS_SELECTOR, self.task_selector)
            logger.info(f"Найдено заданий: {len(tasks)}")
            task_data = []
            for task in tasks:
                try:
                    address = task.text.strip()
                    if address:
                        ng_click = task.get_attribute('ng-click')
                        task_id = None
                        if ng_click:
                            match = re.search(r'openRouteTaskInfo\((\d+)\)', ng_click)
                            if match:
                                task_id = match.group(1)
                            else:
                                match = re.search(r'(\d+)', ng_click)
                                if match:
                                    task_id = match.group(1)
                        task_data.append({'element': task, 'address': address, 'task_id': task_id})
                except:
                    continue
            return task_data
        except Exception as e:
            logger.error(f"Ошибка при поиске заданий: {e}")
            return []

    # ---------- ОТКРЫТИЕ МОДАЛЬНОГО ОКНА С ПОВТОРНЫМИ ПОПЫТКАМИ ----------
    def open_task_modal(self, task_element, retries=3):
        """Открытие модального окна с повторными попытками при неудаче"""
        modal_selectors = ["div.modal.fade.ng-scope.ng-isolate-scope.in", "div.modal.in", "div.modal.show"]

        for attempt in range(retries):
            try:
                logger.info(f"Попытка {attempt + 1} открыть модальное окно...")
                task_element.click()
                time.sleep(3)

                # Проверяем, открылось ли окно
                for selector in modal_selectors:
                    try:
                        WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        logger.info(f"✅ Модальное окно открыто (попытка {attempt + 1})")
                        return True
                    except:
                        continue

                # Если не открылось, нажимаем ESC и повторяем
                logger.warning(f"Попытка {attempt + 1} не удалась, нажимаем ESC...")
                self.press_esc_to_close_modal()
                time.sleep(2)

            except StaleElementReferenceException:
                logger.warning(f"Элемент задания устарел при попытке {attempt + 1}, пробуем найти заново")
                # Пытаемся обновить элемент – в данном контексте это сложно, просто пробуем снова с тем же элементом
                self.press_esc_to_close_modal()
                time.sleep(2)
            except Exception as e:
                logger.warning(f"Ошибка при попытке {attempt + 1}: {e}")
                self.press_esc_to_close_modal()
                time.sleep(2)

        logger.error(f"Не удалось открыть модальное окно после {retries} попыток")
        return False

    # ---------- ИЗВЛЕЧЕНИЕ ДАННЫХ ----------
    def extract_task_data(self):
        data = {
            'address': '',
            'container_type': '',
            'problem': '',
            'city_district': '',
            'driver_name': '',
            'vehicle': '',
            'photos_data': []
        }

        # Адрес
        try:
            address_elements = self.driver.find_elements(By.CSS_SELECTOR, "td.info.ng-binding")
            for elem in address_elements:
                text = elem.text.strip()
                if text and len(text) > 10 and ',' in text:
                    data['address'] = text
                    break
        except Exception as e:
            logger.warning(f"Не удалось извлечь адрес: {e}")

        # Тип тары
        try:
            container_spans = self.driver.find_elements(By.CSS_SELECTOR, "span.wm-garbage-type.ng-binding")
            if container_spans:
                data['container_type'] = container_spans[0].text.strip()
            bold_spans = self.driver.find_elements(By.CSS_SELECTOR, "span[style*='font-weight: bold']")
            for span in bold_spans:
                if span.text.strip():
                    if data['container_type']:
                        data['container_type'] = f"{span.text.strip()} ({data['container_type']})"
                    else:
                        data['container_type'] = span.text.strip()
                    break
            if not data['container_type'] and 'ТБО' in self.driver.page_source:
                data['container_type'] = 'ТБО'
        except:
            pass

        # Проблематика
        try:
            problem_selectors = [
                "span.alert.ng-binding.ng-scope",
                "span.alert",
                "span.text-danger",
                "//span[contains(text(), 'Затруднен')]",
                "//span[contains(text(), 'проблем')]"
            ]
            for selector in problem_selectors:
                try:
                    if selector.startswith('//'):
                        problem_elements = self.driver.find_elements(By.XPATH, selector)
                    else:
                        problem_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for elem in problem_elements:
                        text = elem.text.strip()
                        if text and len(text) > 3:
                            first_line = text.split('\n')[0].strip()
                            if 'Асланов' in first_line or 'И. Х.' in first_line:
                                parts = first_line.split(' ')
                                problem_text = ' '.join(
                                    [p for p in parts if not any(name in p for name in ['Асланов', 'И.', 'Х.'])])
                            else:
                                problem_text = first_line
                            data['problem'] = problem_text.upper()
                            break
                    if data['problem']:
                        break
                except:
                    continue
        except:
            pass

        # Городской округ (запасной вариант)
        try:
            all_elements = self.driver.find_elements(By.XPATH,
                                                     "//*[contains(text(), 'Подольск') or contains(text(), 'округ') or contains(text(), 'Московская')]")
            for elem in all_elements:
                text = elem.text.strip()
                if text and 3 < len(text) < 50:
                    data['city_district'] = text
                    break
            if not data['city_district'] and data['address']:
                address_parts = data['address'].split(',')
                if len(address_parts) > 2:
                    for part in address_parts:
                        if 'округ' in part or 'Подольск' in part:
                            data['city_district'] = part.strip()
                            break
        except:
            pass

        # ФИО и ТС
        try:
            logger.info("Поиск ФИО и ТС...")
            elements_with_slash = self.driver.find_elements(By.XPATH, "//*[contains(text(), '/')]")
            for elem in elements_with_slash:
                text = elem.text.strip()
                if text and '/' in text:
                    text = ' '.join(text.split())
                    if re.search(r'[А-Я]\d{3}[А-Я]{2}\d{2,3}', text) or re.search(r'[А-Я]\d{3}[А-Я]\d{2,3}', text):
                        parts = text.split('/')
                        if len(parts) == 2:
                            vehicle_part = parts[0].strip()
                            driver_part = parts[1].strip()
                            vehicle_match = re.search(r'([А-Я]\d{3}[А-Я]{2}\d{2,3}|[А-Я]\d{3}[А-Я]\d{2,3})',
                                                      vehicle_part)
                            if vehicle_match:
                                data['vehicle'] = vehicle_match.group(1)
                            else:
                                data['vehicle'] = vehicle_part.split()[0] if vehicle_part else ''
                            name_match = re.search(r'([А-Я][а-яё]+ [А-Я]\. ?[А-Я]\.)', driver_part)
                            if name_match:
                                data['driver_name'] = name_match.group(1)
                            else:
                                name_words = driver_part.split()
                                if len(name_words) >= 3:
                                    data['driver_name'] = ' '.join(name_words[:3])
                                else:
                                    data['driver_name'] = driver_part
                            logger.info(f"✅ Найдены: ТС={data['vehicle']}, ФИО={data['driver_name']}")
                            break
            if not data.get('vehicle') or not data.get('driver_name'):
                if not data.get('vehicle'):
                    vehicle_patterns = [r'[А-Я]\d{3}[А-Я]{2}\d{2,3}', r'[А-Я]\d{3}[А-Я]\d{2,3}']
                    for pat in vehicle_patterns:
                        m = re.search(pat, self.driver.page_source)
                        if m:
                            data['vehicle'] = m.group(0)
                            break
                if not data.get('driver_name'):
                    name_patterns = [r'[А-Я][а-яё]+ [А-Я]\. [А-Я]\.', r'[А-Я][а-яё]+ [А-Я]\.[А-Я]\.']
                    for pat in name_patterns:
                        m = re.search(pat, self.driver.page_source)
                        if m:
                            data['driver_name'] = m.group(0)
                            break
            logger.info(f"Результат: ТС='{data.get('vehicle', '')}', ФИО='{data.get('driver_name', '')}'")
        except Exception as e:
            logger.warning(f"Не удалось извлечь ФИО и ТС: {e}")

        # ---------- ИЗВЛЕЧЕНИЕ ФОТО ТОЛЬКО ЧЕРЕЗ CANVAS ----------
        try:
            logger.info("Извлечение фото через canvas (JavaScript)...")
            js_script = """
            var images = document.getElementsByTagName('img');
            var imageData = [];
            for (var i = 0; i < images.length; i++) {
                var img = images[i];
                if (img.src && img.src.includes('routeTaskFileInfo')) {
                    try {
                        var canvas = document.createElement('canvas');
                        var ctx = canvas.getContext('2d');
                        canvas.width = img.naturalWidth;
                        canvas.height = img.naturalHeight;
                        ctx.drawImage(img, 0, 0);
                        var dataUrl = canvas.toDataURL('image/jpeg');
                        imageData.push(dataUrl);
                    } catch(e) {}
                }
            }
            return imageData;
            """
            base64_images = self.driver.execute_script(js_script)
            if base64_images:
                logger.info(f"Найдено {len(base64_images)} изображений через canvas")
                for i, base64_img in enumerate(base64_images):
                    try:
                        if ',' in base64_img:
                            base64_data = base64_img.split(',')[1]
                            photo_data = base64.b64decode(base64_data)
                            if len(photo_data) > 1024:
                                data['photos_data'].append(photo_data)
                                self.stats['photos_captured'] += 1
                                if self.config['save_photos_locally']:
                                    photo_filename = f"canvas_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{i}.jpg"
                                    photo_path = self.photos_dir / photo_filename
                                    with open(photo_path, 'wb') as f:
                                        f.write(photo_data)
                            else:
                                self.stats['photos_failed'] += 1
                        else:
                            self.stats['photos_failed'] += 1
                    except Exception as e:
                        logger.warning(f"Ошибка декодирования фото {i + 1}: {e}")
                        self.stats['photos_failed'] += 1
            else:
                logger.info("Фото через canvas не найдены")
                self.stats['photos_failed'] += 1
        except Exception as e:
            logger.error(f"Ошибка при извлечении фото через canvas: {e}")

        logger.info(f"Всего извлечено фото: {len(data['photos_data'])}")
        return data

    # ---------- ОПРЕДЕЛЕНИЕ ЦЕЛЕВЫХ ЧАТОВ ----------
    def get_target_chats(self, district):
        if not district or district == '':
            logger.warning("Городской округ не определен, задание не будет отправлено в Telegram")
            return []

        district_lower = district.lower()

        if '#н/д' in district_lower or '#н/д' == district_lower or 'н/д' in district_lower:
            logger.info("Обнаружен #Н/Д, отправка во все чаты")
            chats = []
            for key in ['podolsk', 'chekhov', 'south']:
                if self.chat_ids[key]:
                    chats.append(self.chat_ids[key])
            return chats

        for keyword, chat_key in self.district_to_chat_key.items():
            if keyword in district_lower:
                chat_id = self.chat_ids.get(chat_key)
                if chat_id:
                    return [chat_id]
                else:
                    logger.warning(f"Чат для округа '{district}' не настроен, задание не будет отправлено")
                    return []

        logger.warning(f"Городской округ '{district}' не распознан, задание не будет отправлено в Telegram")
        return []

    # ---------- ОТПРАВКА В TELEGRAM ----------
    def format_telegram_message(self, task_data):
        try:
            lines = []
            if task_data.get('address'):
                lines.append(f"📍 Адрес: {task_data['address']}")
            if task_data.get('container_type'):
                lines.append(f"🗑️ Тип тары: {task_data['container_type']}")
            if task_data.get('problem'):
                lines.append(f"⚠️ <b>Проблематика: {task_data['problem']}</b>")
            if task_data.get('city_district'):
                lines.append(f"🏙️ Городской округ: {task_data['city_district']}")
            if task_data.get('driver_name'):
                driver_name = ' '.join(task_data['driver_name'].split())
                lines.append(f"👤 ФИО: {driver_name}")
            if task_data.get('vehicle'):
                lines.append(f"🚛 ТС: {task_data['vehicle']}")
            photos_count = len(task_data.get('photos_data', []))
            if photos_count > 0:
                lines.append(f"📸 Фото: {photos_count} шт.")
            lines.append(f"\n⏰ Обработано: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            return '\n'.join(lines)
        except Exception as e:
            logger.error(f"Ошибка форматирования сообщения: {e}")
            return "Ошибка при формировании сообщения"

    def send_photos_with_caption_to_chat(self, chat_id, photos_data, caption):
        if not self.telegram_bot.enabled or not chat_id:
            return False
        try:
            valid_photos = []
            for i, pd in enumerate(photos_data):
                if pd and len(pd) > 1024:
                    valid_photos.append(pd)
                else:
                    logger.warning(f"Фото {i + 1} невалидно, пропуск")
            if not valid_photos:
                return False

            if self.config['send_media_group'] and len(valid_photos) > 1:
                logger.info(f"Отправка {len(valid_photos)} фото медиагруппой в чат {chat_id}...")
                success = self.telegram_bot.send_media_group_bytes_to_chat(chat_id, valid_photos, caption)
                if success:
                    self.stats['media_groups_sent'] += 1
                    self.stats['photos_sent'] += len(valid_photos)
                return success
            else:
                if len(valid_photos) == 1:
                    success = self.telegram_bot.send_photo_bytes_to_chat(chat_id, valid_photos[0], caption=caption)
                    if success:
                        self.stats['single_photos_sent'] += 1
                        self.stats['photos_sent'] += 1
                    return success
                else:
                    all_ok = True
                    success = self.telegram_bot.send_photo_bytes_to_chat(chat_id, valid_photos[0], caption=caption)
                    if success:
                        self.stats['single_photos_sent'] += 1
                        self.stats['photos_sent'] += 1
                    else:
                        all_ok = False
                    for pd in valid_photos[1:]:
                        time.sleep(0.5)
                        if self.telegram_bot.send_photo_bytes_to_chat(chat_id, pd):
                            self.stats['single_photos_sent'] += 1
                            self.stats['photos_sent'] += 1
                        else:
                            all_ok = False
                    return all_ok
        except Exception as e:
            logger.error(f"Ошибка отправки фото в чат {chat_id}: {e}")
            return False

    def send_to_telegram(self, task_data):
        try:
            message = self.format_telegram_message(task_data)
            photos_data = task_data.get('photos_data', [])
            district = task_data.get('city_district', '')

            target_chats = self.get_target_chats(district)
            if not target_chats:
                logger.info("Нет целевых чатов для отправки, пропускаем отправку в Telegram")
                return False

            telegram_sent = False
            for chat_id in target_chats:
                if not chat_id:
                    continue
                try:
                    if photos_data:
                        success = self.send_photos_with_caption_to_chat(chat_id, photos_data, message)
                        if success:
                            self.stats['sent_to_telegram'] += 1
                            telegram_sent = True
                            if chat_id == self.chat_ids['podolsk']:
                                self.stats['telegram_podolsk'] += 1
                                self.add_to_report('podolsk', task_data)
                            elif chat_id == self.chat_ids['chekhov']:
                                self.stats['telegram_chekhov'] += 1
                                self.add_to_report('chekhov', task_data)
                            elif chat_id == self.chat_ids['south']:
                                self.stats['telegram_south'] += 1
                                self.add_to_report('south', task_data)
                    else:
                        success = self.telegram_bot.send_message_to_chat(chat_id, message)
                        if success:
                            self.stats['sent_to_telegram'] += 1
                            telegram_sent = True
                            if chat_id == self.chat_ids['podolsk']:
                                self.stats['telegram_podolsk'] += 1
                                self.add_to_report('podolsk', task_data)
                            elif chat_id == self.chat_ids['chekhov']:
                                self.stats['telegram_chekhov'] += 1
                                self.add_to_report('chekhov', task_data)
                            elif chat_id == self.chat_ids['south']:
                                self.stats['telegram_south'] += 1
                                self.add_to_report('south', task_data)
                except Exception as e:
                    logger.error(f"Ошибка отправки в чат {chat_id}: {e}")

            return telegram_sent
        except Exception as e:
            logger.error(f"Ошибка отправки в Telegram: {e}")
            return False

    def add_to_report(self, chat_key, task_data):
        driver = task_data.get('driver_name', 'Неизвестно')
        vehicle = task_data.get('vehicle', 'Неизвестно')
        problem = task_data.get('problem', 'Не указана')
        key = (driver, vehicle)
        self.report_stats[chat_key][key][problem] += 1

    # ---------- ОТПРАВКА ОТЧЁТА ----------
    def send_reports(self):
        logger.info("Формирование периодических отчётов...")
        print("\n  📊 Формирование отчётов за последние 3 часа...")

        for chat_key, chat_id in self.chat_ids.items():
            if not chat_id:
                continue

            stats = self.report_stats[chat_key]
            if not stats:
                report_text = (
                    f"<b>📊 ОТЧЁТ ЗА ПЕРИОД</b>\n\n"
                    f"<i>{self.last_report_time.strftime('%Y-%m-%d %H:%M:%S')} – {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>\n\n"
                    f"За указанный период не было обработано ни одного задания."
                )
            else:
                lines = [
                    f"<b>📊 ОТЧЁТ ЗА ПЕРИОД</b>",
                    f"<i>{self.last_report_time.strftime('%Y-%m-%d %H:%M:%S')} – {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>\n"
                ]

                for (driver, vehicle), problems in stats.items():
                    lines.append(f"<b>{driver}</b> ({vehicle}):")
                    for problem, count in problems.items():
                        lines.append(f"  • {problem}: {count}")
                    lines.append("")

                report_text = "\n".join(lines)

            self.telegram_bot.send_message_to_chat(chat_id, report_text)
            logger.info(f"✅ Отчёт отправлен в чат {chat_key}")
            self.stats['reports_sent'] += 1

        self.report_stats = {
            'podolsk': defaultdict(lambda: defaultdict(int)),
            'chekhov': defaultdict(lambda: defaultdict(int)),
            'south': defaultdict(lambda: defaultdict(int))
        }
        self.last_report_time = datetime.now()

    # ---------- ЗАКРЫТИЕ МОДАЛЬНОГО ОКНА ----------
    def press_esc_to_close_modal(self):
        try:
            for _ in range(3):
                try:
                    self.driver.switch_to.active_element.send_keys(Keys.ESCAPE)
                except:
                    self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                time.sleep(0.5)
            time.sleep(1)
            modal_selectors = ["div.modal.fade.ng-scope.ng-isolate-scope.in", "div.modal.in", "div.modal.show"]
            for sel in modal_selectors:
                try:
                    if self.driver.find_element(By.CSS_SELECTOR, sel).is_displayed():
                        logger.warning("Модальное окно не закрылось после ESC")
                        return False
                except:
                    pass
            logger.info("✅ Модальное окно закрыто ESC")
            return True
        except Exception as e:
            logger.error(f"Ошибка при нажатии ESC: {e}")
            return False

    # ---------- СОХРАНЕНИЕ ДАННЫХ ----------
    def save_task_data(self, task_data):
        success_google = False
        success_csv = False

        if task_data.get('photos_data'):
            task_data['photos_str'] = f"Canvas: {len(task_data['photos_data'])} фото"
        else:
            task_data['photos_str'] = 'Нет фото'

        if self.google_sheets and self.google_sheets.worksheet:
            if self.google_sheets.add_row(task_data):
                success_google = True
                self.stats['saved_to_google'] += 1
                if task_data.get('city_district') and task_data.get('city_district') != task_data.get(
                        '_original_city_district', ''):
                    self.stats['vlookup_matches'] += 1
                else:
                    self.stats['vlookup_misses'] += 1

        if self.csv_manager.add_row(task_data):
            success_csv = True
            self.stats['saved_to_csv'] += 1

        try:
            filename = f"backup_{datetime.now().strftime('%Y%m%d')}.json"
            task_data['backup_timestamp'] = datetime.now().isoformat()
            json_data = task_data.copy()
            if 'photos_data' in json_data:
                json_data['photos_count'] = len(json_data['photos_data'])
                del json_data['photos_data']
            existing = []
            if Path(filename).exists():
                with open(filename, 'r', encoding='utf-8') as f:
                    existing = json.load(f)
            existing.append(json_data)
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(existing, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Не удалось сохранить JSON: {e}")

        return success_google or success_csv

    # ---------- ОБРАБОТКА ЗАДАНИЯ ----------
    def process_task(self, task_info, is_retry=False):
        task_id = task_info.get('task_id', 'unknown')
        address = task_info.get('address', 'Без адреса')
        task_key = f"{task_id}_{hash(address)}"

        if task_key in self.processed_tasks:
            logger.info(f"Задание {task_id} уже обработано, пропускаем")
            return False

        logger.info(f"{'ПОВТОРНАЯ ' if is_retry else ''}Обработка задания: {address[:50]}...")
        print(f"  {'🔄' if is_retry else '📝'} Обработка: {address[:40]}...")

        try:
            # Используем новую функцию open_task_modal с повторными попытками
            if not self.open_task_modal(task_info['element']):
                logger.error(f"Не удалось открыть модальное окно для задания {task_id}")
                return False

            task_data = self.extract_task_data()
            task_data['task_id'] = task_id
            task_data['_original_city_district'] = task_data.get('city_district', '')

            photos_ok = len(task_data.get('photos_data', [])) > 0

            if not photos_ok:
                logger.warning(f"Задание {task_id}: фото не получены")
                if is_retry:
                    logger.error(
                        f"Задание {task_id} не удалось обработать после повторных попыток, помечаем как перманентную ошибку")
                    self.stats['tasks_failed_permanent'] += 1
                    self.press_esc_to_close_modal()
                    return False
                else:
                    self.failed_tasks[task_key] = {
                        'attempts': self.failed_tasks.get(task_key, {}).get('attempts', 0) + 1,
                        'last_seen': time.time(),
                        'task_info': task_info
                    }
                    logger.info(
                        f"Задание {task_id} добавлено в список на повторную проверку (попытка {self.failed_tasks[task_key]['attempts']})")
                    self.press_esc_to_close_modal()
                    return False
            else:
                if not self.save_task_data(task_data):
                    logger.error(f"❌ Не удалось сохранить задание {task_id}")
                    self.press_esc_to_close_modal()
                    return False

                telegram_sent = False
                if self.telegram_bot.enabled:
                    telegram_sent = self.send_to_telegram(task_data)
                    task_data['telegram_sent'] = 'Да' if telegram_sent else 'Нет'
                else:
                    task_data['telegram_sent'] = 'Бот отключен'

                self.press_esc_to_close_modal()

                self.processed_tasks.add(task_key)
                if task_key in self.failed_tasks:
                    del self.failed_tasks[task_key]

                self.stats['tasks_processed'] += 1
                if is_retry:
                    self.stats['tasks_retried'] += 1

                logger.info(f"✅ Задание {task_id} успешно обработано")
                print(f"    ✅ Обработано")
                if task_data.get('driver_name'):
                    print(f"    👤 {task_data['driver_name']}")
                if task_data.get('vehicle'):
                    print(f"    🚛 {task_data['vehicle']}")
                if task_data.get('problem'):
                    print(f"    ⚠️  {task_data['problem'][:40]}...")
                print(f"    🏙️ Округ: {task_data.get('city_district', 'Не определен')}")
                print(f"    📸 Получено фото: {len(task_data.get('photos_data', []))}")
                if telegram_sent:
                    print(f"    📤 Отправлено в Telegram")
                return True

        except Exception as e:
            logger.error(f"❌ Ошибка обработки задания {task_id}: {e}", exc_info=True)
            self.stats['errors'] += 1
            try:
                self.press_esc_to_close_modal()
            except:
                pass
            return False

    def retry_failed_tasks(self):
        if not self.failed_tasks:
            return
        current_time = time.time()
        tasks_to_remove = []
        logger.info(f"Попытка повторной обработки {len(self.failed_tasks)} заданий...")
        print(f"\n  🔄 Повторная обработка {len(self.failed_tasks)} заданий...")
        for task_key, fail_info in list(self.failed_tasks.items()):
            if fail_info['attempts'] >= self.config['max_retry_attempts']:
                logger.warning(
                    f"Задание {task_key} превысило лимит попыток ({self.config['max_retry_attempts']}), удаляем из очереди")
                tasks_to_remove.append(task_key)
                self.stats['tasks_failed_permanent'] += 1
                continue
            if current_time - fail_info['last_seen'] > 3600:
                logger.info(f"Задание {task_key} не появлялось более часа, удаляем из очереди")
                tasks_to_remove.append(task_key)
                continue
            task_info = fail_info['task_info']
            try:
                task_info['element'].is_displayed()
                self.process_task(task_info, is_retry=True)
            except StaleElementReferenceException:
                logger.warning(f"Элемент задания {task_key} устарел, пробуем найти заново")
                new_task = self.find_task_by_id(task_info.get('task_id'))
                if new_task:
                    task_info['element'] = new_task
                    self.process_task(task_info, is_retry=True)
                else:
                    logger.warning(f"Не удалось найти задание {task_key} на странице")
                    tasks_to_remove.append(task_key)
        for key in tasks_to_remove:
            if key in self.failed_tasks:
                del self.failed_tasks[key]

    def find_task_by_id(self, task_id):
        if not task_id:
            return None
        tasks = self.find_all_tasks()
        for task in tasks:
            if task.get('task_id') == task_id:
                return task['element']
        return None

    # ---------- ОСНОВНОЙ ЦИКЛ ----------
    def monitor_tasks(self):
        logger.info("🚀 ЗАПУСК МОНИТОРИНГА ЗАДАНИЙ")
        logger.info(f"📊 Интервал проверки: {self.config['monitor_interval']} сек")
        logger.info("⏱️ Мониторинг без ограничения по времени (до остановки пользователем)")
        logger.info(f"📅 Интервал отчётов: {self.config['report_interval_hours']} ч")

        self.monitoring_active = True
        start_time = time.time()
        check_count = 0
        self.last_report_time = datetime.now()

        print("\n" + "=" * 60)
        print("МОНИТОРИНГ ЗАДАНИЙ АКТИВЕН")
        print("Фото извлекаются ТОЛЬКО через canvas")
        print("Городской округ определяется по Лист2 (VLOOKUP)")
        print("Рассылка в Telegram по округам (Подольск, Чехов, Южный кластер)")
        print("#Н/Д отправляется во все три чата")
        print("При ошибке открытия окна - нажатие ESC и повтор (до 3 раз)")
        print("Неудачные задания повторяются")
        print(f"Отчёты формируются каждые {self.config['report_interval_hours']} ч")
        print("=" * 60 + "\n")

        try:
            # Бесконечный цикл без проверки длительности
            while self.monitoring_active:
                check_count += 1
                self.stats['total_checks'] = check_count
                elapsed = time.time() - start_time
                hours = int(elapsed // 3600)
                minutes = int((elapsed % 3600) // 60)
                seconds = int(elapsed % 60)

                logger.info(f"Проверка #{check_count} (работаем: {hours:02d}:{minutes:02d}:{seconds:02d})")
                print(
                    f"\n[#{check_count}] {datetime.now().strftime('%H:%M:%S')} (работы: {hours:02d}:{minutes:02d}:{seconds:02d})")

                try:
                    if check_count % 5 == 1:
                        self.driver.refresh()
                        time.sleep(5)
                        self.switch_to_routes_tab()
                        time.sleep(3)

                    tasks = self.find_all_tasks()
                    tasks_found = len(tasks)
                    self.stats['tasks_found'] += tasks_found
                    print(f"  📋 Найдено заданий: {tasks_found}")

                    processed_this_round = 0
                    for i, task in enumerate(tasks, 1):
                        task_key = f"{task.get('task_id')}_{hash(task.get('address', ''))}"
                        if task_key in self.processed_tasks:
                            continue
                        print(f"  🔄 Обработка {i}/{tasks_found}")
                        if self.process_task(task, is_retry=False):
                            processed_this_round += 1
                        time.sleep(1.5)

                    print(f"  ✅ Обработано в этом цикле: {processed_this_round}/{tasks_found}")

                    if self.failed_tasks:
                        self.retry_failed_tasks()

                    # Проверяем, не пора ли отправить отчёт
                    time_since_last_report = (datetime.now() - self.last_report_time).total_seconds()
                    if time_since_last_report >= self.config['report_interval_hours'] * 3600:
                        self.send_reports()

                    print(
                        f"  📈 Статистика: Всего обработано {self.stats['tasks_processed']}, Ошибок: {self.stats['errors']}")
                    print(
                        f"  📊 Сохранено в Google: {self.stats['saved_to_google']}, в CSV: {self.stats['saved_to_csv']}")
                    if self.telegram_bot.enabled:
                        print(f"  📤 Отправлено в Telegram: {self.stats['sent_to_telegram']}")
                        print(
                            f"      Подольск: {self.stats['telegram_podolsk']}, Чехов: {self.stats['telegram_chekhov']}, Юг: {self.stats['telegram_south']}")
                    print(
                        f"  📸 Получено фото (canvas): {self.stats['photos_captured']}, отправлено фото: {self.stats['photos_sent']}")
                    print(
                        f"  🎞️ Медиагрупп: {self.stats['media_groups_sent']}, одиночных: {self.stats['single_photos_sent']}")
                    print(
                        f"  🏙️ VLOOKUP: совпадений {self.stats['vlookup_matches']}, пропусков {self.stats['vlookup_misses']}")
                    print(f"  ⏳ В очереди на повтор: {len(self.failed_tasks)}")
                    print(
                        f"  📅 Следующий отчёт через: {max(0, self.config['report_interval_hours'] * 3600 - time_since_last_report):.0f} сек")

                except Exception as e:
                    logger.error(f"Ошибка в цикле проверки: {e}", exc_info=True)
                    self.stats['errors'] += 1

                # Интервал между проверками
                sleep_time = self.config['monitor_interval']
                print(f"  ⏳ Следующая проверка через {sleep_time} сек...")
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Мониторинг прерван пользователем")
            print("\n\n🛑 МОНИТОРИНГ ПРЕРВАН ПОЛЬЗОВАТЕЛЕМ")

        finally:
            self.monitoring_active = False
            total_time = time.time() - start_time
            hours = int(total_time // 3600)
            minutes = int((total_time % 3600) // 60)
            seconds = int(total_time % 60)

            logger.info(f"Мониторинг завершен. Время работы: {hours:02d}:{minutes:02d}:{seconds:02d}")
            print("\n" + "=" * 60)
            print("МОНИТОРИНГ ЗАДАНИЙ ЗАВЕРШЕН")
            print(f"Время работы: {hours:02d}:{minutes:02d}:{seconds:02d}")
            print(f"Выполнено проверок: {self.stats['total_checks']}")
            print(f"Найдено заданий: {self.stats['tasks_found']}")
            print(f"Обработано заданий: {self.stats['tasks_processed']}")
            print(f"Повторно обработано: {self.stats['tasks_retried']}")
            print(f"Перманентных ошибок: {self.stats['tasks_failed_permanent']}")
            print(f"Сохранено в Google: {self.stats['saved_to_google']}")
            print(f"Сохранено в CSV: {self.stats['saved_to_csv']}")
            if self.telegram_bot.enabled:
                print(f"Отправлено в Telegram: {self.stats['sent_to_telegram']}")
                print(f"  • Подольск: {self.stats['telegram_podolsk']}")
                print(f"  • Чехов: {self.stats['telegram_chekhov']}")
                print(f"  • Южный кластер: {self.stats['telegram_south']}")
                print(f"  • Отчётов отправлено: {self.stats['reports_sent']}")
            print(f"Фото получено (canvas): {self.stats['photos_captured']}")
            print(f"Фото отправлено: {self.stats['photos_sent']}")
            print(f"Медиагрупп: {self.stats['media_groups_sent']}, одиночных: {self.stats['single_photos_sent']}")
            print(f"VLOOKUP совпадений: {self.stats['vlookup_matches']}, пропусков: {self.stats['vlookup_misses']}")
            print(f"Ошибок: {self.stats['errors']}")
            print("=" * 60)

            # Отправляем финальный отчёт (за последний период)
            self.send_reports()

    def start_monitoring(self):
        try:
            print("1. Настройка WebDriver...")
            if not self.setup_driver():
                print("❌ Не удалось настроить WebDriver")
                return False

            print("2. Авторизация на сайте...")
            if not self.login():
                print("❌ Не удалось авторизоваться")
                return False

            print("3. Переход на страницу мониторинга...")
            if not self.navigate_to_monitor_page():
                print("⚠️ Не удалось перейти на страницу мониторинга")

            if self.google_sheets and self.google_sheets.worksheet:
                print("4. Google Sheets подключены ✓")
                if self.google_sheets.lookup_worksheet:
                    print(f"   Загружено {len(self.google_sheets.address_district_map)} записей из Лист2")
                else:
                    print("   ⚠️ Лист2 не найден, VLOOKUP работать не будет")
            else:
                print("4. Google Sheets не подключены, используются локальные файлы")

            if self.telegram_bot.enabled:
                print("5. Telegram бот подключен ✓")
                print(f"   Чат Подольск: {self.chat_ids['podolsk']}")
                print(f"   Чат Чехов: {self.chat_ids['chekhov']}")
                print(f"   Чат Южный кластер: {self.chat_ids['south']}")

                test_msg = (
                    f"<b>🤖 Система мониторинга запущена.</b>\n\n"
                )
                for chat_id in self.chat_ids.values():
                    if chat_id:
                        self.telegram_bot.send_message_to_chat(chat_id, test_msg)
            else:
                print("5. Telegram бот отключен (проверьте .env)")

            print("\n" + "=" * 60)
            print("ВСЕ СИСТЕМЫ ГОТОВЫ")
            print("Фото ТОЛЬКО через canvas")
            print("Городской округ: VLOOKUP (Лист2)")
            print("Рассылка: Подольск, Чехов, Юг, #Н/Д -> все три чата")
            print("При ошибке открытия окна: ESC + повтор (до 3 раз)")
            print("Повторная обработка неудачных заданий")
            print(f"Отчёты: каждые {self.config['report_interval_hours']} ч в каждый чат")
            print("=" * 60 + "\n")

            self.monitor_tasks()
            return True

        except Exception as e:
            logger.error(f"Критическая ошибка: {e}", exc_info=True)
            print(f"\n🔥 Критическая ошибка: {e}")
            return False
        finally:
            self.close_driver()

    def close_driver(self):
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Драйвер закрыт")
            except:
                pass
            finally:
                self.driver = None


# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
def main():
    try:
        print("=" * 60)
        print("МОНИТОРИНГ ЗАДАНИЙ (ФОТО ЧЕРЕЗ CANVAS + VLOOKUP + РАССЫЛКА ПО ОКРУГАМ)")
        print("=" * 60)

        print("\nПроверка зависимостей:")
        print("  • selenium")
        print("  • webdriver-manager")
        print("  • python-dotenv")
        print("  • requests ✓")

        if GOOGLE_SHEETS_AVAILABLE:
            print("  • gspread, google-auth ✓")
        else:
            print("  • gspread, google-auth (НЕ УСТАНОВЛЕНЫ)")

        print("\n" + "=" * 60)
        print("\nКонфигурация мониторинга (используется .env или значения по умолчанию):")
        print("   Интервал проверки: 5 секунд (задаётся в .env или по умолчанию)")
        print("   Длительность: без ограничений")
        print("1. Запустить мониторинг")
        print("2. Выйти")

        choice = input("\nВыберите действие (1-2): ").strip()
        if choice != "1":
            print("Выход.")
            return 0

        monitor = ElementMonitor()

        print(f"\n⚙️ Настройки:")
        print(f"   Интервал проверки: {monitor.config['monitor_interval']} сек")
        print(f"   Логирование: {monitor.config['log_level']}")
        print(f"   Telegram: {'Да' if monitor.telegram_bot.enabled else 'Нет'}")
        print(f"   Медиагруппа: {'Да' if monitor.config['send_media_group'] else 'Нет'}")
        print(f"   Локальное сохранение фото: {'Да' if monitor.config['save_photos_locally'] else 'Нет'}")
        print(f"   Повторные попытки: {monitor.config['max_retry_attempts']}")
        print(f"   Интервал отчётов: {monitor.config['report_interval_hours']} ч")

        print("\n" + "-" * 60)
        print("ВАЖНО:")
        print("1. Фото извлекаются ТОЛЬКО через canvas (JavaScript)")
        print("2. Городской округ определяется по Лист2 (VLOOKUP)")
        print("3. В Google Sheets в столбец L вставляется формула =VLOOKUP(Cn;'Лист2'!A:B;2;0)")
        print("4. Рассылка в Telegram:")
        print("   - г.о. Подольск → чат №1")
        print("   - г.о. Чеховский → чат №2")
        print("   - г.о. Серпухов, Пущино, Протвино → чат №3")
        print("   - #Н/Д → все три чата")
        print("5. При ошибке открытия модального окна: нажатие ESC и повтор до 3 раз")
        print("6. Неудачные задания автоматически повторяются")
        print(f"7. Отчёты по водителям отправляются каждые {monitor.config['report_interval_hours']} ч в каждый чат")
        print("-" * 60)

        confirm = input("\nЗапустить мониторинг? (y/n): ").lower()
        if confirm != 'y':
            print("Мониторинг отменен")
            return 0

        print("\n" + "=" * 60)
        print("ЗАПУСК МОНИТОРИНГА")
        print("Фото через canvas, округ через VLOOKUP, рассылка по округам, отчёты каждые 3 ч")
        print("=" * 60)

        success = monitor.start_monitoring()
        print(f"\n{'✅' if success else '❌'} Мониторинг завершен {'успешно' if success else 'с ошибками'}")
        return 0
    except Exception as e:
        print(f"\n🔥 Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()

    print("\n" + "=" * 60)
    print("ПРОГРАММА ЗАВЕРШЕНА")
    print("=" * 60)
    print("\nСозданные файлы:")
    print("  • logs/ - папка с логами")
    print("  • monitoring_data.csv - данные")
    print("  • backup_YYYYMMDD.json - резервная копия")
    print("  • monitoring_report.json - отчет")
    print("  • debug_logs/ - отладка")
    print("  • downloaded_photos/ - фото (если включено)")

    if not GOOGLE_SHEETS_AVAILABLE:
        print("\n⚠️ Для Google Sheets: pip install gspread google-auth")

    input("\nНажмите Enter для выхода...")
    sys.exit(exit_code)