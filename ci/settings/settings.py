S3_BUCKET_NAME = "clickhouse-builds"
S3_REPORT_BUCKET_NAME = "clickhouse-test-reports"
S3_BUCKET_HTTP_ENDPOINT = "clickhouse-builds.s3.amazonaws.com"
S3_REPORT_BUCKET_HTTP_ENDPOINT = "s3.amazonaws.com/clickhouse-test-reports"

MAIN_BRANCH = "master"

S3_ARTIFACT_PATH = f"{S3_BUCKET_NAME}"
CI_CONFIG_RUNS_ON = ["self-hosted", "style-checker-aarch64"]
# TODO: cannot crosscompile the image: clickhouse/mysql-java-client. use amd runner to have all images for amd:
DOCKER_BUILD_RUNS_ON = ["self-hosted", "style-checker"]
CACHE_S3_PATH = f"{S3_BUCKET_NAME}/ci_ch_cache"
HTML_S3_PATH = f"{S3_REPORT_BUCKET_NAME}"
S3_BUCKET_TO_HTTP_ENDPOINT = {
    S3_BUCKET_NAME: S3_BUCKET_HTTP_ENDPOINT,
    S3_REPORT_BUCKET_NAME: S3_REPORT_BUCKET_HTTP_ENDPOINT,
}
ENABLE_ARTIFACTS_REPORT = True

COMPRESS_THRESHOLD_MB = 32
TEXT_CONTENT_EXTENSIONS = [".txt", ".log", ".err", ".out", ".tsv", ".csv"]

DOCKERHUB_USERNAME = "robotclickhouse"
DOCKERHUB_SECRET = "dockerhub_robot_password"

CI_DB_DB_NAME = "default"
CI_DB_TABLE_NAME = "checks"
SECRET_CI_DB_URL = "clickhouse-test-stat-url"
SECRET_CI_DB_USER = "clickhouse-test-stat-login"
SECRET_CI_DB_PASSWORD = "clickhouse-test-stat-password"

USE_CUSTOM_GH_AUTH = True
SECRET_GH_APP_ID: str = "woolenwolf_gh_app.clickhouse-app-id"
SECRET_GH_APP_PEM_KEY: str = "woolenwolf_gh_app.clickhouse-app-key"

INSTALL_PYTHON_REQS_FOR_NATIVE_JOBS = ""

DISABLED_WORKFLOWS = [
    "new_pull_request.py",
    "defs.py",
    "job_configs.py",
]
PYTHONPATHS = ""
DEFAULT_LOCAL_TEST_WORKFLOW = "PRNEW"
READY_FOR_MERGE_CUSTOM_STATUS_NAME = "Mergeable Check"
