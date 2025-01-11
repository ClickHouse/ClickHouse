import dataclasses
import importlib.util
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Union


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
    DOCKER_BUILD_JOB_NAME = "Docker Builds"
    FINISH_WORKFLOW_JOB_NAME = "Finish Workflow"
    READY_FOR_MERGE_STATUS_NAME = "Ready for Merge"
    CI_CONFIG_RUNS_ON: Optional[List[str]] = None
    DOCKER_BUILD_RUNS_ON: Optional[List[str]] = None
    VALIDATE_FILE_PATHS: bool = True
    PIPELINE_PRECHECKS: Optional[List[Union[str, callable]]] = None

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
    INSTALL_PYTHON_FOR_NATIVE_JOBS: bool = False
    INSTALL_PYTHON_REQS_FOR_NATIVE_JOBS: str = "./ci/requirements.txt"
    ENVIRONMENT_VAR_FILE: str = f"{TEMP_DIR}/environment.json"
    RUN_LOG: str = f"{TEMP_DIR}/praktika_run.log"

    SECRET_GH_APP_ID: str = "GH_APP_ID"
    SECRET_GH_APP_PEM_KEY: str = "GH_APP_PEM_KEY"

    ENV_SETUP_SCRIPT: str = f"{TEMP_DIR}/praktika_setup_env.sh"
    WORKFLOW_STATUS_FILE: str = f"{TEMP_DIR}/workflow_status.json"

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
    HTML_PAGE_FILE: str = "./praktika/json.html"
    TEXT_CONTENT_EXTENSIONS: Iterable[str] = frozenset([".txt", ".log"])
    S3_BUCKET_TO_HTTP_ENDPOINT: Optional[Dict[str, str]] = None

    DOCKERHUB_USERNAME: str = ""
    DOCKERHUB_SECRET: str = ""

    ######################################
    #        CI DB Settings              #
    ######################################
    SECRET_CI_DB_URL: str = "CI_DB_URL"
    SECRET_CI_DB_PASSWORD: str = "CI_DB_PASSWORD"
    CI_DB_DB_NAME = ""
    CI_DB_TABLE_NAME = ""
    CI_DB_INSERT_TIMEOUT_SEC = 5

    DISABLE_MERGE_COMMIT = True


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
    "DOCKER_BUILD_RUNS_ON",
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
    "READY_FOR_MERGE_STATUS_NAME",
    "SECRET_CI_DB_URL",
    "SECRET_CI_DB_PASSWORD",
    "CI_DB_DB_NAME",
    "CI_DB_TABLE_NAME",
    "CI_DB_INSERT_TIMEOUT_SEC",
    "SECRET_GH_APP_PEM_KEY",
    "SECRET_GH_APP_ID",
    "MAIN_BRANCH",
    "DISABLE_MERGE_COMMIT",
    "PIPELINE_PRECHECKS",
]


def _get_settings() -> _Settings:
    res = _Settings()

    directory = Path(_Settings.SETTINGS_DIRECTORY)
    for py_file in directory.glob("*.py"):
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
