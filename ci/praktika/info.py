import json
import os
import urllib
from pathlib import Path
from typing import Optional

from praktika.runtime import RunConfig
from praktika.settings import Settings


class Info:

    def __init__(self):
        from ._environment import _Environment

        self.env = _Environment.get()

    @property
    def sha(self):
        return self.env.SHA

    @property
    def pr_number(self):
        return self.env.PR_NUMBER

    @property
    def workflow_name(self):
        return self.env.WORKFLOW_NAME

    @property
    def pr_body(self):
        return self.env.PR_BODY

    @property
    def pr_title(self):
        return self.env.PR_TITLE

    @property
    def pr_url(self):
        return self.env.CHANGE_URL

    @property
    def commit_url(self):
        return self.env.COMMIT_URL

    @property
    def git_branch(self):
        return self.env.BRANCH

    @property
    def git_sha(self):
        return self.env.SHA

    @property
    def repo_name(self):
        return self.env.REPOSITORY

    @property
    def fork_name(self):
        return self.env.FORK_NAME

    @property
    def user_name(self):
        return self.env.USER_LOGIN

    @property
    def pr_labels(self):
        return self.env.PR_LABELS

    @property
    def instance_type(self):
        return self.env.INSTANCE_TYPE

    @property
    def instance_id(self):
        return self.env.INSTANCE_ID

    @property
    def is_local_run(self):
        return self.env.LOCAL_RUN

    def get_report_url(self, latest=False):
        sha = self.env.SHA
        if latest:
            sha = "latest"
        return self.get_specific_report_url(
            pr_number=self.env.PR_NUMBER, branch=self.env.BRANCH, sha=sha
        )

    def dump(self):
        self.env.dump()

    def get_specific_report_url(self, pr_number, branch, sha, job_name=""):
        from praktika.settings import Settings

        if pr_number:
            ref_param = f"PR={pr_number}"
        else:
            assert branch
            ref_param = f"REF={branch}"
        path = Settings.HTML_S3_PATH
        for bucket, endpoint in Settings.S3_BUCKET_TO_HTTP_ENDPOINT.items():
            if bucket in path:
                path = path.replace(bucket, endpoint)
                break
        res = f"https://{path}/{Path(Settings.HTML_PAGE_FILE).name}?{ref_param}&sha={sha}&name_0={urllib.parse.quote(self.env.WORKFLOW_NAME, safe='')}"
        if job_name:
            res += f"&name_1={urllib.parse.quote(job_name, safe='')}"
        return res

    @staticmethod
    def get_workflow_input_value(input_name) -> Optional[str]:
        from praktika.settings import _Settings

        try:
            with open(_Settings.WORKFLOW_INPUTS_FILE, "r", encoding="utf8") as f:
                input_obj = json.load(f)
                return input_obj[input_name]
        except Exception as e:
            print(f"ERROR: Exception, while reading workflow input [{e}]")
        return None

    def store_custom_data(self, key, value):
        assert (
            self.env.JOB_NAME == "Config Workflow"
        ), "Custom data can be stored only in Config Workflow Job"
        custom_data = {key: value}
        if Path(Settings.CUSTOM_DATA_FILE).is_file():
            with open(Settings.CUSTOM_DATA_FILE, "r", encoding="utf8") as f:
                custom_data = json.load(f)
                custom_data[key] = value
        with open(Settings.CUSTOM_DATA_FILE, "w", encoding="utf8") as f:
            json.dump(custom_data, f, indent=4)

    def get_custom_data(self, key=None):
        if key:
            return RunConfig.from_fs(self.env.WORKFLOW_NAME).custom_data.get(key, None)
        return RunConfig.from_fs(self.env.WORKFLOW_NAME).custom_data
