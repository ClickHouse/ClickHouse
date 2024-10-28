import dataclasses
import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Type

from praktika import Workflow
from praktika._settings import _Settings
from praktika.utils import MetaClasses, T


@dataclasses.dataclass
class _Environment(MetaClasses.Serializable):
    WORKFLOW_NAME: str
    JOB_NAME: str
    REPOSITORY: str
    BRANCH: str
    SHA: str
    PR_NUMBER: int
    EVENT_TYPE: str
    JOB_OUTPUT_STREAM: str
    EVENT_FILE_PATH: str
    CHANGE_URL: str
    COMMIT_URL: str
    BASE_BRANCH: str
    RUN_ID: str
    RUN_URL: str
    INSTANCE_TYPE: str
    INSTANCE_ID: str
    INSTANCE_LIFE_CYCLE: str
    LOCAL_RUN: bool = False
    PARAMETER: Any = None
    REPORT_INFO: List[str] = dataclasses.field(default_factory=list)
    name = "environment"

    @classmethod
    def file_name_static(cls, _name=""):
        return f"{_Settings.TEMP_DIR}/{cls.name}.json"

    @classmethod
    def from_dict(cls: Type[T], obj: Dict[str, Any]) -> T:
        JOB_OUTPUT_STREAM = os.getenv("GITHUB_OUTPUT", "")
        obj["JOB_OUTPUT_STREAM"] = JOB_OUTPUT_STREAM
        if "PARAMETER" in obj:
            obj["PARAMETER"] = _to_object(obj["PARAMETER"])
        return cls(**obj)

    def add_info(self, info):
        self.REPORT_INFO.append(info)
        self.dump()

    @classmethod
    def get(cls):
        if Path(cls.file_name_static()).is_file():
            return cls.from_fs("environment")
        else:
            print("WARNING: Environment: get from env")
            env = cls.from_env()
            env.dump()
            return env

    def set_job_name(self, job_name):
        self.JOB_NAME = job_name
        self.dump()
        return self

    @staticmethod
    def get_needs_statuses():
        if Path(_Settings.WORKFLOW_STATUS_FILE).is_file():
            with open(_Settings.WORKFLOW_STATUS_FILE, "r", encoding="utf8") as f:
                return json.load(f)
        else:
            print(
                f"ERROR: Status file [{_Settings.WORKFLOW_STATUS_FILE}] does not exist"
            )
            raise RuntimeError()

    @classmethod
    def from_env(cls) -> "_Environment":
        WORKFLOW_NAME = os.getenv("GITHUB_WORKFLOW", "")
        JOB_NAME = os.getenv("JOB_NAME", "")
        REPOSITORY = os.getenv("GITHUB_REPOSITORY", "")
        BRANCH = os.getenv("GITHUB_HEAD_REF", "")

        EVENT_FILE_PATH = os.getenv("GITHUB_EVENT_PATH", "")
        JOB_OUTPUT_STREAM = os.getenv("GITHUB_OUTPUT", "")
        RUN_ID = os.getenv("GITHUB_RUN_ID", "0")
        RUN_URL = f"https://github.com/{REPOSITORY}/actions/runs/{RUN_ID}"
        BASE_BRANCH = os.getenv("GITHUB_BASE_REF", "")

        if EVENT_FILE_PATH:
            with open(EVENT_FILE_PATH, "r", encoding="utf-8") as f:
                github_event = json.load(f)
            if "pull_request" in github_event:
                EVENT_TYPE = Workflow.Event.PULL_REQUEST
                PR_NUMBER = github_event["pull_request"]["number"]
                SHA = github_event["pull_request"]["head"]["sha"]
                CHANGE_URL = github_event["pull_request"]["html_url"]
                COMMIT_URL = CHANGE_URL + f"/commits/{SHA}"
            elif "commits" in github_event:
                EVENT_TYPE = Workflow.Event.PUSH
                SHA = github_event["after"]
                CHANGE_URL = github_event["head_commit"]["url"]  # commit url
                PR_NUMBER = 0
                COMMIT_URL = CHANGE_URL
            else:
                assert False, "TODO: not supported"
        else:
            print("WARNING: Local execution - dummy Environment will be generated")
            SHA = "TEST"
            PR_NUMBER = -1
            EVENT_TYPE = Workflow.Event.PUSH
            CHANGE_URL = ""
            COMMIT_URL = ""

        INSTANCE_TYPE = (
            os.getenv("INSTANCE_TYPE", None)
            # or Shell.get_output("ec2metadata --instance-type")
            or ""
        )
        INSTANCE_ID = (
            os.getenv("INSTANCE_ID", None)
            # or Shell.get_output("ec2metadata --instance-id")
            or ""
        )
        INSTANCE_LIFE_CYCLE = (
            os.getenv("INSTANCE_LIFE_CYCLE", None)
            # or Shell.get_output(
            #     "curl -s --fail http://169.254.169.254/latest/meta-data/instance-life-cycle"
            # )
            or ""
        )

        return _Environment(
            WORKFLOW_NAME=WORKFLOW_NAME,
            JOB_NAME=JOB_NAME,
            REPOSITORY=REPOSITORY,
            BRANCH=BRANCH,
            EVENT_FILE_PATH=EVENT_FILE_PATH,
            JOB_OUTPUT_STREAM=JOB_OUTPUT_STREAM,
            SHA=SHA,
            EVENT_TYPE=EVENT_TYPE,
            PR_NUMBER=PR_NUMBER,
            RUN_ID=RUN_ID,
            CHANGE_URL=CHANGE_URL,
            COMMIT_URL=COMMIT_URL,
            RUN_URL=RUN_URL,
            BASE_BRANCH=BASE_BRANCH,
            INSTANCE_TYPE=INSTANCE_TYPE,
            INSTANCE_ID=INSTANCE_ID,
            INSTANCE_LIFE_CYCLE=INSTANCE_LIFE_CYCLE,
            REPORT_INFO=[],
        )

    def get_s3_prefix(self, latest=False):
        return self.get_s3_prefix_static(self.PR_NUMBER, self.BRANCH, self.SHA, latest)

    @classmethod
    def get_s3_prefix_static(cls, pr_number, branch, sha, latest=False):
        prefix = ""
        if pr_number > 0:
            prefix += f"{pr_number}"
        else:
            prefix += f"{branch}"
        if latest:
            prefix += f"/latest"
        elif sha:
            prefix += f"/{sha}"
        return prefix

    # TODO: find a better place for the function. This file should not import praktika.settings
    #   as it's requires reading users config, that's why imports nested inside the function
    def get_report_url(self):
        import urllib

        from praktika.settings import Settings
        from praktika.utils import Utils

        path = Settings.HTML_S3_PATH
        for bucket, endpoint in Settings.S3_BUCKET_TO_HTTP_ENDPOINT.items():
            if bucket in path:
                path = path.replace(bucket, endpoint)
                break
        REPORT_URL = f"https://{path}/{Path(Settings.HTML_PAGE_FILE).name}?PR={self.PR_NUMBER}&sha={self.SHA}&name_0={urllib.parse.quote(self.WORKFLOW_NAME, safe='')}&name_1={urllib.parse.quote(self.JOB_NAME, safe='')}"
        return REPORT_URL

    def is_local_run(self):
        return self.LOCAL_RUN


def _to_object(data):
    if isinstance(data, dict):
        return SimpleNamespace(**{k: _to_object(v) for k, v in data.items()})
    elif isinstance(data, list):
        return [_to_object(i) for i in data]
    else:
        return data
