import requests


def _resolve_token(token_or_fn):
    """Accept either a string token or a callable returning one; return the
    current string. Lets long-running callers pass a ``GHTokenProvider`` and
    one-shot callers (tests, ``orchestrate job``) pass a plain token."""
    return token_or_fn() if callable(token_or_fn) else token_or_fn


class CheckRun:
    """Top-level workflow GitHub check run."""

    @staticmethod
    def _api(method, url, token, json_body=None):
        resp = requests.request(
            method,
            url,
            headers={
                "Authorization": f"Bearer {_resolve_token(token)}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            json=json_body,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    @classmethod
    def start(cls, token, repo, head_sha, name, details_url=None, with_cancel_action=True):
        body = {
            "name": name,
            "head_sha": head_sha,
            "status": "in_progress",
        }
        if with_cancel_action:
            body["actions"] = [
                {
                    "label": "Cancel",
                    "description": "Cancel this CI run",
                    "identifier": "cancel",
                }
            ]
        if details_url is not None:
            body["details_url"] = details_url
        data = cls._api(
            "POST",
            f"https://api.github.com/repos/{repo}/check-runs",
            token,
            body,
        )
        return cls(token, repo, data["id"], name)

    def __init__(self, token, repo, id, name):
        self.token = token
        self.repo = repo
        self.id = id
        self.name = name

    def retitle(self, name, details_url=None, with_cancel_action=True):
        """Adopt a check run opened before the workflow name was known
        (e.g. the controller's pre-clone bootstrap check): rename it to the
        real workflow name, keep it ``in_progress``, and re-attach the Cancel
        action and report URL. Idempotent PATCH on the same check-run id."""
        body = {"name": name, "status": "in_progress"}
        if with_cancel_action:
            body["actions"] = [
                {
                    "label": "Cancel",
                    "description": "Cancel this CI run",
                    "identifier": "cancel",
                }
            ]
        if details_url is not None:
            body["details_url"] = details_url
        self._api(
            "PATCH",
            f"https://api.github.com/repos/{self.repo}/check-runs/{self.id}",
            self.token,
            body,
        )
        self.name = name
        return self

    def complete(self, conclusion, output=None, details_url=None):
        body = {"status": "completed", "conclusion": conclusion}
        if output is not None:
            body["output"] = output
        if details_url is not None:
            body["details_url"] = details_url
        self._api(
            "PATCH",
            f"https://api.github.com/repos/{self.repo}/check-runs/{self.id}",
            self.token,
            body,
        )

    def update(self, output=None, details_url=None, status=None, conclusion=None):
        body = {}
        if status is not None:
            body["status"] = status
        if conclusion is not None:
            body["conclusion"] = conclusion
        if output is not None:
            body["output"] = output
        if details_url is not None:
            body["details_url"] = details_url
        if not body:
            return
        self._api(
            "PATCH",
            f"https://api.github.com/repos/{self.repo}/check-runs/{self.id}",
            self.token,
            body,
        )
