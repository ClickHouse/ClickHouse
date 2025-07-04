import dataclasses
import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Type

from . import Job, Workflow
from .settings import Settings
from .utils import MetaClasses, Shell, T


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
    PR_BODY: str
    PR_TITLE: str
    USER_LOGIN: str
    FORK_NAME: str
    # merged PR for "push" or "merge_group" workflow
    LINKED_PR_NUMBER: int = 0
    LOCAL_RUN: bool = False
    PR_LABELS: List[str] = dataclasses.field(default_factory=list)
    REPORT_INFO: List[str] = dataclasses.field(default_factory=list)
    JOB_CONFIG: Optional[Job.Config] = None
    TRACEBACKS: List[str] = dataclasses.field(default_factory=list)
    name = "environment"

    @classmethod
    def from_env(cls) -> "_Environment":
        WORKFLOW_NAME = os.getenv("GITHUB_WORKFLOW", "")
        JOB_NAME = os.getenv("JOB_NAME", "")
        REPOSITORY = os.getenv("GITHUB_REPOSITORY", "")
        # GITHUB_HEAD_REF for pull_request, GITHUB_REF_NAME for push
        BRANCH = os.getenv("GITHUB_HEAD_REF", "") or os.getenv("GITHUB_REF_NAME", "")

        EVENT_FILE_PATH = os.getenv("GITHUB_EVENT_PATH", "")
        JOB_OUTPUT_STREAM = os.getenv("GITHUB_OUTPUT", "")
        RUN_ID = os.getenv("GITHUB_RUN_ID", "0")
        RUN_URL = f"https://github.com/{REPOSITORY}/actions/runs/{RUN_ID}"
        BASE_BRANCH = os.getenv("GITHUB_BASE_REF", "")
        USER_LOGIN = ""
        FORK_NAME = ""
        PR_BODY = ""
        PR_TITLE = ""
        PR_LABELS = []
        LINKED_PR_NUMBER = 0

        if EVENT_FILE_PATH:
            with open(EVENT_FILE_PATH, "r", encoding="utf-8") as f:
                github_event = json.load(f)
            if "pull_request" in github_event:
                FORK_NAME = github_event["pull_request"]["head"]["repo"]["full_name"]
                EVENT_TYPE = Workflow.Event.PULL_REQUEST
                PR_NUMBER = int(github_event["pull_request"]["number"])
                SHA = github_event["pull_request"]["head"]["sha"]
                CHANGE_URL = github_event["pull_request"]["html_url"]
                COMMIT_URL = CHANGE_URL + f"/commits/{SHA}"
                PR_BODY = github_event["pull_request"]["body"]
                PR_TITLE = github_event["pull_request"]["title"]
                PR_LABELS = [
                    label["name"] for label in github_event["pull_request"]["labels"]
                ]
                USER_LOGIN = github_event["pull_request"]["user"]["login"]
            elif "commits" in github_event:
                EVENT_TYPE = Workflow.Event.PUSH
                SHA = github_event["after"]
                CHANGE_URL = github_event["head_commit"]["url"]  # commit url
                PR_NUMBER = 0
                COMMIT_URL = CHANGE_URL
                commit_message = github_event["head_commit"]["message"]
                parts = commit_message.split(
                    "pull request #"
                )  # Merge pull request #77106 from XXXX
                if len(parts) >= 2:
                    pr_str = parts[1].split()[0]
                    LINKED_PR_NUMBER = int(pr_str) if pr_str.isdigit() else 0
            elif "schedule" in github_event:
                EVENT_TYPE = Workflow.Event.SCHEDULE
                SHA = os.getenv(
                    "GITHUB_SHA", "0000000000000000000000000000000000000000"
                )
                PR_NUMBER = 0
                CHANGE_URL = (
                    github_event["repository"]["html_url"] + "/commit/" + SHA
                )  # commit url
                COMMIT_URL = CHANGE_URL
            elif "inputs" in github_event:
                # assume this is a dispatch
                EVENT_TYPE = Workflow.Event.DISPATCH
                SHA = os.getenv(
                    "GITHUB_SHA", "0000000000000000000000000000000000000000"
                )
                PR_NUMBER = 0
                CHANGE_URL = (
                    github_event["repository"]["html_url"] + "/commit/" + SHA
                )  # commit url
                COMMIT_URL = CHANGE_URL
            elif "merge_group" in github_event:
                PR_NUMBER = 0
                EVENT_TYPE = Workflow.Event.MERGE_QUEUE
                SHA = os.getenv(
                    "GITHUB_SHA", "0000000000000000000000000000000000000000"
                )
                head_ref = github_event.get("merge_group", {}).get("head_ref", "")
                try:
                    if "/pr-" in head_ref:
                        pr_number_part = head_ref.split("/pr-")[1].split("-")[0]
                        LINKED_PR_NUMBER = (
                            int(pr_number_part) if pr_number_part.isdigit() else 0
                        )
                    CHANGE_URL = (
                        f"{github_event['repository']['html_url']}/pull/{LINKED_PR_NUMBER}"
                        if LINKED_PR_NUMBER
                        else ""
                    )
                except:
                    LINKED_PR_NUMBER = 0
                    CHANGE_URL = ""

                COMMIT_URL = CHANGE_URL
            else:
                assert False, "TODO: not supported"

            INSTANCE_TYPE = (
                os.getenv("INSTANCE_TYPE", None)
                or Shell.get_output("ec2metadata --instance-type")
                or ""
            )
            INSTANCE_ID = (
                os.getenv("INSTANCE_ID", None)
                or Shell.get_output("ec2metadata --instance-id")
                or ""
            )
            INSTANCE_LIFE_CYCLE = (
                os.getenv("INSTANCE_LIFE_CYCLE", None)
                or Shell.get_output(
                    "curl -s --fail http://169.254.169.254/latest/meta-data/instance-life-cycle"
                )
                or ""
            )

        else:
            print("WARNING: Local execution - dummy Environment will be generated")
            SHA = "TEST"
            PR_NUMBER = -1
            EVENT_TYPE = Workflow.Event.PUSH
            CHANGE_URL = ""
            COMMIT_URL = ""
            INSTANCE_TYPE = ""
            INSTANCE_ID = ""
            INSTANCE_LIFE_CYCLE = ""

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
            PR_BODY=PR_BODY,
            PR_TITLE=PR_TITLE,
            USER_LOGIN=USER_LOGIN,
            FORK_NAME=FORK_NAME,
            PR_LABELS=PR_LABELS,
            INSTANCE_LIFE_CYCLE=INSTANCE_LIFE_CYCLE,
            REPORT_INFO=[],
            LINKED_PR_NUMBER=LINKED_PR_NUMBER,
        )

    @classmethod
    def file_name_static(cls, _name=""):
        return f"{Settings.TEMP_DIR}/{cls.name}.json"

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
        if Path(Settings.WORKFLOW_STATUS_FILE).is_file():
            with open(Settings.WORKFLOW_STATUS_FILE, "r", encoding="utf8") as f:
                return json.load(f)
        else:
            print(
                f"ERROR: Status file [{Settings.WORKFLOW_STATUS_FILE}] does not exist"
            )
            raise RuntimeError()

    def get_s3_prefix(self, latest=False):
        return self.get_s3_prefix_static(self.PR_NUMBER, self.BRANCH, self.SHA, latest)

    @classmethod
    def get_s3_prefix_static(cls, pr_number, branch, sha, latest=False):
        assert pr_number or branch
        if pr_number:
            prefix = f"PRs/{pr_number}"
        else:
            prefix = f"REFs/{branch}"
        assert sha or latest
        assert pr_number >= 0
        if latest:
            prefix += f"/latest"
        elif sha:
            prefix += f"/{sha}"
        return prefix

    def is_local_run(self):
        return self.LOCAL_RUN


def _to_object(data):
    if isinstance(data, dict):
        return SimpleNamespace(**{k: _to_object(v) for k, v in data.items()})
    elif isinstance(data, list):
        return [_to_object(i) for i in data]
    else:
        return data
