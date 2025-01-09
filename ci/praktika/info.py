from ._environment import _Environment


class Info:

    def __init__(self):
        pass

    @property
    def pr_body(self):
        return _Environment.get().PR_BODY
