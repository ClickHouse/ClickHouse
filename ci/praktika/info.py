import json
import urllib
from pathlib import Path
from typing import Optional


class Info:

    def __init__(self):
        from ._environment import _Environment

        self.env = _Environment.get()

    @property
    def pr_body(self):
        return self.env.PR_BODY

    @property
    def pr_title(self):
        return self.env.PR_TITLE

    @property
    def pr_number(self):
        return self.env.PR_NUMBER

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

    def get_report_url(self, latest=False):
        from praktika.settings import Settings

        path = Settings.HTML_S3_PATH
        for bucket, endpoint in Settings.S3_BUCKET_TO_HTTP_ENDPOINT.items():
            if bucket in path:
                path = path.replace(bucket, endpoint)
                break
        if self.env.PR_NUMBER:
            if latest:
                sha = "latest"
            else:
                sha = self.env.SHA
        else:
            sha = self.env.SHA
        return f"https://{path}/{Path(Settings.HTML_PAGE_FILE).name}?PR={self.env.PR_NUMBER}&sha={sha}&name_0={urllib.parse.quote(self.env.WORKFLOW_NAME, safe='')}"

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
