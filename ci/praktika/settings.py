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

    ENABLE_ARTIFACTS_REPORT: bool = False

    ######################################
    #    Runtime Settings                #
    ######################################
    MAX_RETRIES_S3 = 3
    MAX_RETRIES_GH = 3

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

    USE_CUSTOM_GH_AUTH: bool = False
    SECRET_GH_APP_ID: str = ""
    SECRET_GH_APP_PEM_KEY: str = ""

    ENV_SETUP_SCRIPT: str = f"{TEMP_DIR}/praktika_setup_env.sh"
    WORKFLOW_STATUS_FILE: str = f"{TEMP_DIR}/workflow_status.json"
    WORKFLOW_INPUTS_FILE: str = f"{TEMP_DIR}/workflow_inputs.json"
    ARTIFACT_URLS_FILE: str = f"{TEMP_DIR}/artifact_urls.json"

    ######################################
    #        CI Cache settings           #
    ######################################
    CACHE_VERSION: int = 1
    CACHE_DIGEST_LEN: int = 20
    CACHE_S3_PATH: str = ""
    CACHE_LOCAL_PATH: str = f"{TEMP_DIR}/ci_cache"

    ######################################
    #        Report settings             #
    ######################################
    HTML_S3_PATH: str = ""
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
    CI_DB_INSERT_TIMEOUT_SEC = 5

    # to post links for reading statistics in html report (with read-only user)
    CI_DB_READ_USER: str = ""
    CI_DB_READ_URL: str = ""


_USER_DEFINED_SETTINGS = [
    "S3_ARTIFACT_PATH",
    "CACHE_S3_PATH",
    "HTML_S3_PATH",
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
    "VALIDATE_FILE_PATHS",
    "DOCKERHUB_USERNAME",
    "DOCKERHUB_SECRET",
    "READY_FOR_MERGE_CUSTOM_STATUS_NAME",
    "SECRET_CI_DB_URL",
    "SECRET_CI_DB_USER",
    "SECRET_CI_DB_PASSWORD",
    "CI_DB_DB_NAME",
    "CI_DB_TABLE_NAME",
    "CI_DB_INSERT_TIMEOUT_SEC",
    "USE_CUSTOM_GH_AUTH",
    "SECRET_GH_APP_ID",
    "SECRET_GH_APP_PEM_KEY",
    "MAIN_BRANCH",
    "DISABLED_WORKFLOWS",
    "ENABLED_WORKFLOWS",
    "PYTHONPATHS",
    "ENABLE_ARTIFACTS_REPORT",
    "DEFAULT_LOCAL_TEST_WORKFLOW",
    "COMPRESS_THRESHOLD_MB",
    "CI_DB_READ_USER",
    "CI_DB_READ_URL",
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
        spec = importlib.util.spec_from_file_location(
            module_name, f"{_Settings.SETTINGS_DIRECTORY}/{module_name}"
        )
        assert spec
        foo = importlib.util.module_from_spec(spec)
        assert spec.loader
        spec.loader.exec_module(foo)
        for setting in _USER_DEFINED_SETTINGS:
            try:
                value = getattr(foo, setting)
                res.__setattr__(setting, value)
                # print(f"- read user defined setting [{setting} = {value}]")
            except Exception as e:
                # print(f"Exception while read user settings: {e}")
                pass

    return res


class GHRunners:
    ubuntu = "ubuntu-latest"


Settings = _get_settings()
