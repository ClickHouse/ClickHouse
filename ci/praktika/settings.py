import dataclasses
import importlib.util
from pathlib import Path
from typing import Dict, Iterable, List, Optional


@dataclasses.dataclass
class _Settings:
    ######################################
    #    Pipeline generation settings    #
    ######################################
    MAIN_BRANCH = "main"
    CI_PATH = "./ci"
    WORKFLOW_PATH_PREFIX: str = "./.github/workflows"
    WORKFLOWS_DIRECTORY: str = f"{CI_PATH}/workflows"
    SETTINGS_DIRECTORY: str = f"{CI_PATH}/settings"
    CI_CONFIG_JOB_NAME = "Config Workflow"

    # Enables a single job (DOCKER_BUILD_MANIFEST_JOB_NAME) for building all platforms and merge
    ENABLE_MULTIPLATFORM_DOCKER_IN_ONE_JOB = False
    DOCKER_BUILD_ARM_LINUX_JOB_NAME = "Dockers Build (arm)"
    DOCKER_BUILD_AMD_LINUX_JOB_NAME = "Dockers Build (amd)"
    DOCKER_BUILD_MANIFEST_JOB_NAME = "Dockers Build (multiplatform manifest)"
    DOCKER_MERGE_RUNS_ON: Optional[List[str]] = None
    DOCKER_BUILD_ARM_RUNS_ON: Optional[List[str]] = None
    DOCKER_BUILD_AMD_RUNS_ON: Optional[List[str]] = None

    FINISH_WORKFLOW_JOB_NAME = "Finish Workflow"
    READY_FOR_MERGE_CUSTOM_STATUS_NAME = ""
    CI_CONFIG_RUNS_ON: Optional[List[str]] = None
    VALIDATE_FILE_PATHS: bool = True
    DISABLED_WORKFLOWS: Optional[List[str]] = None
    ENABLED_WORKFLOWS: Optional[List[str]] = None
    DEFAULT_LOCAL_TEST_WORKFLOW: str = ""

    ######################################
    #    Runtime Settings                #
    ######################################
    MAX_RETRIES_S3 = 3
    MAX_RETRIES_GH = 3
    # PR label that bypasses all job filtering (filter hooks and changed-file filtering)
    CI_FORCE_ALL_LABEL: str = "ci-force-all"

    ######################################
    #   S3 (artifact storage) settings   #
    ######################################
    S3_ARTIFACT_PATH: str = ""

    ######################################
    #        CI workspace settings       #
    ######################################
    TEMP_DIR: str = "./ci/tmp"
    # TODO: remove if using temp dir for in and out is ok
    OUTPUT_DIR: str = f"{TEMP_DIR}"
    INPUT_DIR: str = f"{TEMP_DIR}"
    PYTHON_INTERPRETER: str = "python3"
    PYTHON_PACKET_MANAGER: str = "pip3"
    PYTHON_VERSION: str = "3.9"
    PYTHONPATHS: str = ""
    INSTALL_PYTHON_FOR_NATIVE_JOBS: bool = False
    INSTALL_PYTHON_REQS_FOR_NATIVE_JOBS: str = "./ci/requirements.txt"
    ENVIRONMENT_VAR_FILE: str = f"{TEMP_DIR}/environment.json"
    RUN_LOG: str = f"{TEMP_DIR}/job.log"

    ######################################
    #      Host metrics (CPU/RAM)        #
    ######################################
    # Sample whole-VM CPU and RAM usage in the background while a job runs and
    # store a decimated timeline in Result.ext["metrics"] (rendered in json.html).
    HOST_METRICS_ENABLED: bool = True
    # Reporting/window interval: one aggregated point (avg + peak) is emitted and
    # written per window, so the timeline stays ~1 point / this-many-seconds
    # regardless of the fine cadence.
    HOST_METRICS_SAMPLE_INTERVAL_SEC: float = 5.0
    # Fine sampling cadence: /proc is read this often within each reporting window
    # so short bursts are captured as the window's peak instead of being averaged
    # away. Must be <= the reporting interval.
    HOST_METRICS_FINE_INTERVAL_SEC: float = 1.0
    # Upper bound on points kept per series after min/max decimation, so the
    # payload injected into the Result stays small regardless of job duration.
    HOST_METRICS_MAX_POINTS: int = 400
    HOST_METRICS_FILE: str = f"{TEMP_DIR}/host_metrics.jsonl"
    # Filesystem whose used% is tracked as the "disk" series. Defaults to the
    # working directory, i.e. the disk the job actually writes to.
    HOST_METRICS_DISK_PATH: str = "."
    # Jobs are labelled over/under-utilized only when they ran at least this
    # long OR ran on a host with more than HOST_METRICS_MIN_LABEL_MEM_GB of RAM;
    # short jobs on small runners are too noisy and not worth right-sizing.
    HOST_METRICS_MIN_LABEL_DURATION_SEC: int = 1800
    HOST_METRICS_MIN_LABEL_MEM_GB: int = 15

    SECRET_GH_APP_ID: str = ""
    SECRET_GH_APP_PEM_KEY: str = ""
    SECRET_GH_APP_INSTALLATION_ID: str = ""
    SECRET_GH_APP_REGION: str = ""
    GH_AUTH_LAMBDA_NAME: str = ""
    GH_AUTH_LAMBDA_REGION: str = ""

    ENV_SETUP_SCRIPT: str = f"{TEMP_DIR}/praktika_setup_env.sh"
    WORKFLOW_JOB_FILE: str = f"{TEMP_DIR}/workflow_job.json"
    WORKFLOW_STATUS_FILE: str = f"{TEMP_DIR}/workflow_status.json"
    WORKFLOW_INPUTS_FILE: str = f"{TEMP_DIR}/workflow_inputs.json"
    ARTIFACT_URLS_FILE: str = f"{TEMP_DIR}/artifact_urls.json"

    ######################################
    #        CI Cache settings           #
    ######################################
    # If enabled, Config Workflow creates a content-addressed .git/modules/ archive
    # in S3. Jobs with needs_submodules=True download it instead of cloning from GitHub.
    ENABLE_SUBMODULE_CACHE: bool = False

    CACHE_VERSION: int = 1
    CACHE_DIGEST_LEN: int = 20
    CACHE_S3_PATH: str = ""
    CACHE_LOCAL_PATH: str = f"{TEMP_DIR}/ci_cache"

    ######################################
    #        Report settings             #
    ######################################
    S3_REPORT_BUCKET: str = ""
    # Optional: upstream report bucket to merge issue catalogs from (e.g. "clickhouse-test-reports")
    S3_UPSTREAM_REPORT_BUCKET: str = ""
    HTML_PAGE_FILE: str = "./ci/praktika/json.html"
    S3_BUCKET_TO_HTTP_ENDPOINT: Optional[Dict[str, str]] = None
    TEXT_CONTENT_EXTENSIONS: Iterable[str] = frozenset([".txt", ".log"])
    # Compress if text file size exceeds this threshold (in MB, 0 - disable compression)
    COMPRESS_THRESHOLD_MB: int = 0

    DOCKERHUB_USERNAME: str = ""
    DOCKERHUB_SECRET: str = ""

    ######################################
    #        CI DB Settings              #
    ######################################
    SECRET_CI_DB_URL: str = ""
    SECRET_CI_DB_USER: str = ""
    SECRET_CI_DB_PASSWORD: str = ""
    CI_DB_DB_NAME = ""
    CI_DB_TABLE_NAME = ""
    KEEPER_STRESS_METRICS_DB_NAME = "keeper_stress_tests"
    KEEPER_STRESS_METRICS_TABLE_NAME = "keeper_metrics_ts"
    CI_DB_INSERT_TIMEOUT_SEC = 20
    CI_DB_QUERY_TIMEOUT_SEC = 60

    # to post links for reading statistics in html report (with read-only user)
    CI_DB_READ_USER: str = ""
    CI_DB_READ_URL: str = ""

    # Substrings to classify test failures. Used to generate helper queries for checking failure history.
    # Not required to cover all failures, but recommended to maximize coverage.
    # Choose values wisely to effectively differentiate between different failure types.
    TEST_FAILURE_PATTERNS: Optional[List[str]] = None

    ######################################
    #        Infrastructure Settings     #
    ######################################
    CLOUD_INFRASTRUCTURE_CONFIG_PATH: str = ""
    AWS_REGION: str = ""
    # S3 path for Slack feed events storage (format: bucket/prefix)
    # Used by EventFeed and FeedSubscription for PR notification subscriptions
    EVENT_FEED_S3_PATH: str = ""


_USER_DEFINED_SETTINGS = [
    "S3_ARTIFACT_PATH",
    "CACHE_S3_PATH",
    "S3_REPORT_BUCKET",
    "S3_UPSTREAM_REPORT_BUCKET",
    "CLOUD_INFRASTRUCTURE_CONFIG_PATH",
    "EVENT_FEED_S3_PATH",
    "AWS_REGION",
    "S3_BUCKET_TO_HTTP_ENDPOINT",
    "TEXT_CONTENT_EXTENSIONS",
    "TEMP_DIR",
    "OUTPUT_DIR",
    "INPUT_DIR",
    "CI_CONFIG_RUNS_ON",
    "DOCKER_MERGE_RUNS_ON",
    "DOCKER_BUILD_ARM_RUNS_ON",
    "DOCKER_BUILD_AMD_RUNS_ON",
    "ENABLE_MULTIPLATFORM_DOCKER_IN_ONE_JOB",
    "CI_CONFIG_JOB_NAME",
    "PYTHON_INTERPRETER",
    "PYTHON_VERSION",
    "PYTHON_PACKET_MANAGER",
    "INSTALL_PYTHON_FOR_NATIVE_JOBS",
    "INSTALL_PYTHON_REQS_FOR_NATIVE_JOBS",
    "MAX_RETRIES_S3",
    "MAX_RETRIES_GH",
    "CI_FORCE_ALL_LABEL",
    "VALIDATE_FILE_PATHS",
    "DOCKERHUB_USERNAME",
    "DOCKERHUB_SECRET",
    "READY_FOR_MERGE_CUSTOM_STATUS_NAME",
    "SECRET_CI_DB_URL",
    "SECRET_CI_DB_USER",
    "SECRET_CI_DB_PASSWORD",
    "CI_DB_DB_NAME",
    "CI_DB_TABLE_NAME",
    "KEEPER_STRESS_METRICS_DB_NAME",
    "KEEPER_STRESS_METRICS_TABLE_NAME",
    "CI_DB_INSERT_TIMEOUT_SEC",
    "SECRET_GH_APP_ID",
    "SECRET_GH_APP_PEM_KEY",
    "SECRET_GH_APP_INSTALLATION_ID",
    "SECRET_GH_APP_REGION",
    "GH_AUTH_LAMBDA_NAME",
    "GH_AUTH_LAMBDA_REGION",
    "MAIN_BRANCH",
    "DISABLED_WORKFLOWS",
    "ENABLED_WORKFLOWS",
    "PYTHONPATHS",
    "DEFAULT_LOCAL_TEST_WORKFLOW",
    "COMPRESS_THRESHOLD_MB",
    "ENABLE_SUBMODULE_CACHE",
    "CI_DB_READ_USER",
    "CI_DB_READ_URL",
    "TEST_FAILURE_PATTERNS",
    "HOST_METRICS_ENABLED",
    "HOST_METRICS_SAMPLE_INTERVAL_SEC",
    "HOST_METRICS_FINE_INTERVAL_SEC",
    "HOST_METRICS_MAX_POINTS",
    "HOST_METRICS_FILE",
    "HOST_METRICS_DISK_PATH",
    "HOST_METRICS_MIN_LABEL_DURATION_SEC",
    "HOST_METRICS_MIN_LABEL_MEM_GB",
]


def _get_settings() -> _Settings:
    res = _Settings()

    directory = Path(_Settings.SETTINGS_DIRECTORY)

    py_files = list(directory.glob("*.py"))
    # Support for overriding settings (if for whatever reason you need to override setting(s) in your fork)
    # Sort: First files without "overrides", then files with "overrides"
    sorted_files = sorted(py_files, key=lambda f: "_overrides" in f.name)

    for py_file in sorted_files:
        module_name = py_file.name.removeprefix(".py")
        spec = importlib.util.spec_from_file_location(module_name, f"{_Settings.SETTINGS_DIRECTORY}/{module_name}")
        assert spec
        foo = importlib.util.module_from_spec(spec)
        assert spec.loader
        spec.loader.exec_module(foo)
        for setting in _USER_DEFINED_SETTINGS:
            try:
                value = getattr(foo, setting)
                res.__setattr__(setting, value)
                # print(f"- read user defined setting [{setting} = {value}]")
            except Exception:
                # print(f"Exception while read user settings: {e}")
                pass

    return res


class GHRunners:
    ubuntu = "ubuntu-latest"


Settings = _get_settings()
