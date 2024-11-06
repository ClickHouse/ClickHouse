import dataclasses
from pathlib import Path
from typing import Dict, Iterable, List, Optional


@dataclasses.dataclass
class _Settings:
    ######################################
    #    Pipeline generation settings    #
    ######################################
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
    TEMP_DIR: str = "/tmp/praktika"
    OUTPUT_DIR: str = f"{TEMP_DIR}/output"
    INPUT_DIR: str = f"{TEMP_DIR}/input"
    PYTHON_INTERPRETER: str = "python3"
    PYTHON_PACKET_MANAGER: str = "pip3"
    PYTHON_VERSION: str = "3.9"
    INSTALL_PYTHON_FOR_NATIVE_JOBS: bool = False
    INSTALL_PYTHON_REQS_FOR_NATIVE_JOBS: str = "./ci/requirements.txt"
    ENVIRONMENT_VAR_FILE: str = f"{TEMP_DIR}/environment.json"
    RUN_LOG: str = f"{TEMP_DIR}/praktika_run.log"

    SECRET_GH_APP_ID: str = "GH_APP_ID"
    SECRET_GH_APP_PEM_KEY: str = "GH_APP_PEM_KEY"

    ENV_SETUP_SCRIPT: str = "/tmp/praktika_setup_env.sh"
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
    DOCKER_WD: str = "/wd"

    ######################################
    #        CI DB Settings              #
    ######################################
    SECRET_CI_DB_URL: str = "CI_DB_URL"
    SECRET_CI_DB_PASSWORD: str = "CI_DB_PASSWORD"
    CI_DB_DB_NAME = ""
    CI_DB_TABLE_NAME = ""
    CI_DB_INSERT_TIMEOUT_SEC = 5


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
]


class GHRunners:
    ubuntu = "ubuntu-latest"


if __name__ == "__main__":
    for setting in _USER_DEFINED_SETTINGS:
        print(_Settings().__getattribute__(setting))
    # print(dataclasses.asdict(_Settings()))
