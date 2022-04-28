import os
from os import path as p

module_dir = p.abspath(p.dirname(__file__))
git_root = p.abspath(p.join(module_dir, "..", ".."))

CI = bool(os.getenv("CI"))
TEMP_PATH = os.getenv("TEMP_PATH", p.abspath(p.join(module_dir, "./tmp")))

CACHES_PATH = os.getenv("CACHES_PATH", TEMP_PATH)
CLOUDFLARE_TOKEN = os.getenv("CLOUDFLARE_TOKEN")
GITHUB_EVENT_PATH = os.getenv("GITHUB_EVENT_PATH", "")
GITHUB_JOB = os.getenv("GITHUB_JOB", "local")
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "Altinity/ClickHouse")
GITHUB_RUN_ID = os.getenv("GITHUB_RUN_ID", "0")
GITHUB_SERVER_URL = os.getenv("GITHUB_SERVER_URL", "https://github.com")
GITHUB_WORKSPACE = os.getenv("GITHUB_WORKSPACE", git_root)
GITHUB_RUN_URL = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/actions/runs/{GITHUB_RUN_ID}"
IMAGES_PATH = os.getenv("IMAGES_PATH", TEMP_PATH)
REPORTS_PATH = os.getenv("REPORTS_PATH", p.abspath(p.join(module_dir, "./reports")))
REPO_COPY = os.getenv("REPO_COPY", git_root)
RUNNER_TEMP = os.getenv("RUNNER_TEMP", p.abspath(p.join(module_dir, "./tmp")))
S3_BUILDS_BUCKET = os.getenv("S3_BUILDS_BUCKET", "altinity-build-artifacts")
S3_TEST_REPORTS_BUCKET = os.getenv("S3_TEST_REPORTS_BUCKET", "altinity-build-artifacts")
CLICKHOUSE_STABLE_VERSION_SUFFIX = os.getenv("CLICKHOUSE_STABLE_VERSION_SUFFIX", "stable")
