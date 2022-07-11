import os

CI = bool(os.getenv("CI"))
TEMP_PATH = os.getenv("TEMP_PATH", os.path.abspath("."))

CACHES_PATH = os.getenv("CACHES_PATH", TEMP_PATH)
CLOUDFLARE_TOKEN = os.getenv("CLOUDFLARE_TOKEN")
GITHUB_EVENT_PATH = os.getenv("GITHUB_EVENT_PATH")
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "ClickHouse/ClickHouse")
GITHUB_RUN_ID = os.getenv("GITHUB_RUN_ID")
GITHUB_SERVER_URL = os.getenv("GITHUB_SERVER_URL", "https://github.com")
GITHUB_WORKSPACE = os.getenv("GITHUB_WORKSPACE", os.path.abspath("../../"))
IMAGES_PATH = os.getenv("IMAGES_PATH")
REPORTS_PATH = os.getenv("REPORTS_PATH", "./reports")
REPO_COPY = os.getenv("REPO_COPY", os.path.abspath("../../"))
RUNNER_TEMP = os.getenv("RUNNER_TEMP", os.path.abspath("./tmp"))
S3_BUILDS_BUCKET = os.getenv("S3_BUILDS_BUCKET", "clickhouse-builds")
S3_TEST_REPORTS_BUCKET = os.getenv("S3_TEST_REPORTS_BUCKET", "clickhouse-test-reports")
