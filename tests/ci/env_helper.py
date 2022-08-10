import os
from os import path as p

from build_download_helper import get_with_retries

module_dir = p.abspath(p.dirname(__file__))
git_root = p.abspath(p.join(module_dir, "..", ".."))

CI = bool(os.getenv("CI"))
TEMP_PATH = os.getenv("TEMP_PATH", p.abspath(p.join(module_dir, "./tmp")))

CACHES_PATH = os.getenv("CACHES_PATH", TEMP_PATH)
CLOUDFLARE_TOKEN = os.getenv("CLOUDFLARE_TOKEN")
GITHUB_EVENT_PATH = os.getenv("GITHUB_EVENT_PATH", "")
GITHUB_JOB = os.getenv("GITHUB_JOB", "local")
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "ClickHouse/ClickHouse")
GITHUB_RUN_ID = os.getenv("GITHUB_RUN_ID", "0")
GITHUB_SERVER_URL = os.getenv("GITHUB_SERVER_URL", "https://github.com")
GITHUB_WORKSPACE = os.getenv("GITHUB_WORKSPACE", git_root)
GITHUB_RUN_URL = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/actions/runs/{GITHUB_RUN_ID}"
IMAGES_PATH = os.getenv("IMAGES_PATH", TEMP_PATH)
REPORTS_PATH = os.getenv("REPORTS_PATH", p.abspath(p.join(module_dir, "./reports")))
REPO_COPY = os.getenv("REPO_COPY", git_root)
RUNNER_TEMP = os.getenv("RUNNER_TEMP", p.abspath(p.join(module_dir, "./tmp")))
S3_BUILDS_BUCKET = os.getenv("S3_BUILDS_BUCKET", "clickhouse-builds")
S3_TEST_REPORTS_BUCKET = os.getenv("S3_TEST_REPORTS_BUCKET", "clickhouse-test-reports")

# These parameters are set only on demand, and only once
_GITHUB_JOB_ID = ""
_GITHUB_JOB_URL = ""


def GITHUB_JOB_ID() -> str:
    global _GITHUB_JOB_ID
    global _GITHUB_JOB_URL
    if _GITHUB_JOB_ID:
        return _GITHUB_JOB_ID
    jobs = []
    while not _GITHUB_JOB_ID:
        response = get_with_retries(
            f"https://api.github.com/repos/{GITHUB_REPOSITORY}/"
            f"actions/runs/{GITHUB_RUN_ID}/jobs?per_page=100"
        )
        data = response.json()
        jobs.extend(data["jobs"])
        for job in data["jobs"]:
            if job["name"] != GITHUB_JOB:
                continue
            _GITHUB_JOB_ID = job["id"]
            _GITHUB_JOB_URL = job["html_url"]
            return _GITHUB_JOB_ID
        if len(jobs) == data["total_count"]:
            _GITHUB_JOB_ID = "0"

    return _GITHUB_JOB_ID


def GITHUB_JOB_URL() -> str:
    GITHUB_JOB_ID()
    return _GITHUB_JOB_URL
