import os

S3_BUCKET_NAME = "altinity-build-artifacts"
S3_REPORT_BUCKET_NAME = "altinity-build-artifacts"
S3_BUCKET_HTTP_ENDPOINT = "altinity-build-artifacts.s3.amazonaws.com"
S3_REPORT_BUCKET_HTTP_ENDPOINT = "altinity-build-artifacts.s3.amazonaws.com"


class RunnerLabels:
    STYLE_CHECK_AMD = ["self-hosted", "altinity-on-demand", "altinity-style-checker"]
    STYLE_CHECK_ARM = [
        "self-hosted",
        "altinity-on-demand",
        "altinity-style-checker-aarch64",
    ]


MAIN_BRANCH = "antalya"
S3_ARTIFACT_PATH = S3_BUCKET_NAME
CI_CONFIG_RUNS_ON = RunnerLabels.STYLE_CHECK_ARM

ENABLE_MULTIPLATFORM_DOCKER_IN_ONE_JOB = False
DOCKER_BUILD_AND_MERGE_RUNS_ON = RunnerLabels.STYLE_CHECK_AMD
DOCKER_BUILD_ARM_RUNS_ON = RunnerLabels.STYLE_CHECK_ARM

CACHE_S3_PATH = f"{S3_BUCKET_NAME}/ci_ch_cache"
HTML_S3_PATH = S3_REPORT_BUCKET_NAME
S3_BUCKET_TO_HTTP_ENDPOINT = {
    S3_BUCKET_NAME: S3_BUCKET_HTTP_ENDPOINT,
    S3_REPORT_BUCKET_NAME: S3_REPORT_BUCKET_HTTP_ENDPOINT,
}
ENABLE_ARTIFACTS_REPORT = True

COMPRESS_THRESHOLD_MB = 32
TEXT_CONTENT_EXTENSIONS = [".txt", ".log", ".err", ".out", ".tsv", ".csv", ".json"]

DOCKERHUB_USERNAME = "altinityinfra"
DOCKERHUB_SECRET = "DOCKER_PASSWORD"

CI_DB_DB_NAME = "gh-data"
CI_DB_TABLE_NAME = "checks"
SECRET_CI_DB_URL = "CLICKHOUSE_TEST_STAT_URL"
SECRET_CI_DB_USER = "CLICKHOUSE_TEST_STAT_LOGIN"
SECRET_CI_DB_PASSWORD = "CLICKHOUSE_TEST_STAT_PASSWORD"

# to post links for reading statistics in html report (with read-only user)
CI_DB_READ_USER: str = ""
CI_DB_READ_URL: str = os.environ.get("CLICKHOUSE_TEST_STAT_URL", "") + "play"

USE_CUSTOM_GH_AUTH = False
# SECRET_GH_APP_ID: str = "woolenwolf_gh_app.clickhouse-app-id"
# SECRET_GH_APP_PEM_KEY: str = "woolenwolf_gh_app.clickhouse-app-key"

INSTALL_PYTHON_REQS_FOR_NATIVE_JOBS = ""

DISABLED_WORKFLOWS = [
    "backport_branches.py",
    "custom_build_praktika.py",
    "release_branches.py",
    "merge_queue.py",
    "hourly.py",
    "optimize_toolchain.py",
    "nightly_coverage.py",
    "nightly_fuzzers.py",
    "nightly_jepsen.py",
    "nightly_statistics.py",
    "VectorSearchStress.py",
    "release_builds.py", # FIXME (strtgbb): workflow still needs to be updated for this version
    "pull_request_community.py", # FIXME (strtgbb): workflow still needs to be updated for this version
]

DEFAULT_LOCAL_TEST_WORKFLOW = "pull_request.py"
READY_FOR_MERGE_CUSTOM_STATUS_NAME = "Mergeable Check"
