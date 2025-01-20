import json
from typing import Optional


class Info:

    def __init__(self):
        from ._environment import _Environment

        self.env = _Environment.get()

    @property
    def pr_body(self):
        return self.env.get().PR_BODY

    @property
    def repo_name(self):
        return self.env.get().REPOSITORY

    @property
    def fork_name(self):
        return self.env.get().FORK_NAME

    @property
    def user_name(self):
        return self.env.get().USER_LOGIN

    @property
    def pr_labels(self):
        return self.env.get().PR_LABELS

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
