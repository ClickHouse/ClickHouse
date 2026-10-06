S3_BUCKET_NAME = "clickhouse-builds"
S3_REPORT_BUCKET_NAME = "clickhouse-test-reports"
S3_BUCKET_HTTP_ENDPOINT = "clickhouse-builds.s3.amazonaws.com"
S3_REPORT_BUCKET_HTTP_ENDPOINT = "s3.amazonaws.com/clickhouse-test-reports"


class RunnerLabels:
    STYLE_CHECK_AMD = ["self-hosted", "style-checker"]
    STYLE_CHECK_ARM = ["self-hosted", "style-checker-aarch64"]


MAIN_BRANCH = "master"
S3_ARTIFACT_PATH = S3_BUCKET_NAME
CI_CONFIG_RUNS_ON = RunnerLabels.STYLE_CHECK_ARM

ENABLE_MULTIPLATFORM_DOCKER_IN_ONE_JOB = False
DOCKER_MERGE_RUNS_ON = RunnerLabels.STYLE_CHECK_AMD
DOCKER_BUILD_ARM_RUNS_ON = RunnerLabels.STYLE_CHECK_ARM
DOCKER_BUILD_AMD_RUNS_ON = RunnerLabels.STYLE_CHECK_AMD

CACHE_S3_PATH = f"{S3_BUCKET_NAME}/ci_ch_cache"
HTML_S3_PATH = S3_REPORT_BUCKET_NAME
S3_BUCKET_TO_HTTP_ENDPOINT = {
    S3_BUCKET_NAME: S3_BUCKET_HTTP_ENDPOINT,
    S3_REPORT_BUCKET_NAME: S3_REPORT_BUCKET_HTTP_ENDPOINT,
}
ENABLE_ARTIFACTS_REPORT = True

COMPRESS_THRESHOLD_MB = 32
TEXT_CONTENT_EXTENSIONS = [
    ".txt",
    ".log",
    ".err",
    ".out",
    ".tsv",
    ".csv",
    ".json",
    ".jsonl",
]

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
    # ""
]

DEFAULT_LOCAL_TEST_WORKFLOW = "pull_request.py"
READY_FOR_MERGE_CUSTOM_STATUS_NAME = "Mergeable Check"

CI_DB_READ_USER = "play"
CI_DB_READ_URL = "https://play.clickhouse.com"

EVENT_FEED_S3_PATH = "clickhouse-test-reports-private/slack_feed"
CLOUD_INFRASTRUCTURE_CONFIG_PATH = "./ci/infra/cloud.py"
AWS_REGION = "us-east-1"

# Substrings used to classify and categorize test failures based on error output.
# Use the following query to find test failures NOT covered by current patterns:
# WITH ['Timeout', 'AssertionError', 'E   assert', 'E   Exception', 'E   NameError', 'Connection refused', 'UnboundLocalError', 'AttributeError', 'Too many retry attempts', 'E   FileNotFoundError', 'E   RuntimeError', 'Connection has been closed', 'E   Failed', 'E   ValueError', 'E   KeyError', 'InternalError', 'syntax error', 'E   TypeError', 'E   OSError', 'Code:', 'ClientError', 'QueryRuntimeException', 'Max retries exceeded with url', 'UnicodeDecodeError', 'Py4JError', 'ConnectionLoss', 'Connection reset by peer', 'Exception:', 'Failure:', 'ExistsError', 'ConnectionError', 'Client Error', 'Errno']
# AS reasons
# SELECT test_context_raw
# FROM checks
# WHERE match(test_name, '^test_')
#   AND test_context_raw != ''
#   AND test_status = 'FAIL'
#   AND check_name LIKE 'Integration%'
#   AND check_start_time > now() - INTERVAL 3 MONTH
#   AND NOT arrayExists(
#         r -> positionCaseInsensitive(ifNull(test_context_raw, ''), r) > 0,
#         reasons
#       )
# LIMIT 100;
TEST_FAILURE_PATTERNS = [
    # Common Functional tests failures
    "Timeout",
    "having stderror",
    "result differs with reference",
    "return code",
    "Test runs too long",
    "having exception in stdout",
    "server died",
    "Test internal error:",
    # Common Integration tests failures
    "AssertionError",
    "E   assert",
    "E   Exception",
    "E   NameError",
    "Connection refused",
    "UnboundLocalError",
    "AttributeError",
    "Too many retry attempts",
    "E   FileNotFoundError",
    "E   RuntimeError",
    "Connection has been closed",
    "E   Failed",
    "E   ValueError",
    "E   KeyError",
    "InternalError",
    "syntax error",
    "E   TypeError",
    "E   OSError",
    "Code:",
    "ClientError",
    "QueryRuntimeException",
    "Max retries exceeded with url",
    "UnicodeDecodeError",
    "Py4JError",
    "ConnectionLoss",
    "Connection reset by peer",
    "Exception:",
    "Failure:",
    "ExistsError",
    "ConnectionError",
    "Client Error",
    "Errno",
]
