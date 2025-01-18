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
