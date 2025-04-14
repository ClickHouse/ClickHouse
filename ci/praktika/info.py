import json
import os
import traceback
import urllib
from pathlib import Path
from typing import Optional

from .runtime import RunConfig
from .settings import Settings


class Info:

    def __init__(self):
        from ._environment import _Environment

        self.env = _Environment.get()
        self.workflow = None

    @property
    def sha(self):
        return self.env.SHA

    @property
    def pr_number(self):
        return self.env.PR_NUMBER

    @property
    def linked_pr_number(self):
        """
        PR associated with the merge commit for Push or Merge Queue workflow
        :return: PR number or 0 if not applicable or not found
        """
        return self.env.LINKED_PR_NUMBER

    @property
    def workflow_name(self):
        return self.env.WORKFLOW_NAME

    @property
    def job_config(self):
        return self.env.JOB_CONFIG

    @property
    def job_name(self):
        return self.env.JOB_NAME

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
    def change_url(self):
        return self.pr_url if self.pr_number else self.commit_url

    @property
    def git_branch(self):
        return self.env.BRANCH

    @property
    def base_branch(self):
        return self.env.BASE_BRANCH

    @property
    def git_sha(self):
        return self.env.SHA

    @property
    def repo_name(self):
        return self.env.REPOSITORY

    @property
    def repo_owner(self):
        return os.getenv("GITHUB_REPOSITORY_OWNER", "")

    @property
    def fork_name(self):
        return self.env.FORK_NAME

    @property
    def user_name(self):
        return self.env.USER_LOGIN

    @property
    def run_url(self):
        return self.env.RUN_URL

    @property
    def pr_labels(self):
        return self.env.PR_LABELS

    @property
    def instance_type(self):
        return self.env.INSTANCE_TYPE

    @property
    def is_merge_queue_event(self):
        return self.env.EVENT_TYPE == "merge_group"

    @property
    def is_push_event(self):
        return self.env.EVENT_TYPE == "push"

    @property
    def instance_lifecycle(self):
        return self.env.INSTANCE_LIFE_CYCLE

    @property
    def instance_id(self):
        return self.env.INSTANCE_ID

    @property
    def is_local_run(self):
        return self.env.LOCAL_RUN

    # TODO: Consider defining secrets outside of workflow as it project data in most of the cases
    def get_secret(self, name):
        from .mangle import _get_workflows

        if not self.workflow:
            self.workflow = _get_workflows(self.env.WORKFLOW_NAME)[0]
        return self.workflow.get_secret(name)

    def get_job_report_url(self, latest=False):
        url = self.get_report_url(latest=latest)
        return url + f"&name_1={urllib.parse.quote(self.env.JOB_NAME, safe='')}"

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
        from .settings import Settings

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
        from .settings import _Settings

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
        workflow_config = RunConfig.from_fs(self.env.WORKFLOW_NAME)
        workflow_config.custom_data[key] = value
        workflow_config.dump()

    def get_custom_data(self, key=None):
        custom_data = RunConfig.from_fs(self.env.WORKFLOW_NAME).custom_data
        if key:
            return custom_data.get(key, None)
        return custom_data

    def store_traceback(self):
        self.env.TRACEBACKS.append(traceback.format_exc())
        self.env.dump()

    def is_workflow_ok(self):
        """
        Experimental function
        :return:
        """
        from .result import Result

        result = Result.from_fs(self.env.WORKFLOW_NAME)
        for subresult in result.results:
            if subresult.name == Settings.FINISH_WORKFLOW_JOB_NAME:
                continue
            if not subresult.is_ok():
                print(f"Job [{subresult.name}] is not ok, status [{subresult.status}]")
                return False
        return True
