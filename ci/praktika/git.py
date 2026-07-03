import re
import shlex

from praktika.utils import Shell


class Git:
    """Minimal git helper using Shell; reads tags for version tweak computation."""

    _TAG_PATTERN = re.compile(
        r"\Av\d{2}([.][1-9]\d*){3}-(new|testing|prestable|stable|lts)\Z"
    )

    @staticmethod
    def is_shallow() -> bool:
        return (
            Shell.get_output("git rev-parse --is-shallow-repository") or ""
        ).strip() == "true"

    @staticmethod
    def get_shortlog(ref: str = "HEAD") -> str:
        return Shell.get_output(f"git shortlog {shlex.quote(ref)} --summary") or ""

    @staticmethod
    def get_commit_sha(ref: str) -> str:
        return Shell.get_output_or_raise(f"git rev-list -n1 {shlex.quote(ref)}")

    @staticmethod
    def tag_exists(name: str) -> bool:
        return bool(
            Shell.get_output(
                f"git rev-parse --verify --quiet refs/tags/{shlex.quote(name)}^{{commit}}"
            )
        )

    @staticmethod
    def branch_exists(name: str) -> bool:
        # A remote-tracking branch: the release job fetches every head into
        # refs/remotes/origin/*, so this is the reliable place to look.
        return Shell.check(
            f"git show-ref --verify --quiet refs/remotes/origin/{shlex.quote(name)}",
            verbose=False,
        )

    @staticmethod
    def push(
        repo: str,
        refspec: str,
        force: bool = False,
        dry_run: bool = False,
        strict: bool = False,
        retries: int = 1,
    ) -> bool:
        """Push `refspec` to `repo` over HTTPS authenticated as the GitHub App.

        Mints the App token inline and clears the inherited http extraheader
        per-command so the tokenized URL — not the checkout's default
        GITHUB_TOKEN — is what authenticates (only an App/PAT push re-triggers
        downstream workflows). The token expands at runtime, so its literal
        `${token}` stays out of the f-string and the URL is assembled by
        concatenation; `repo`/`refspec` are passed shell-quoted. Kept out of the
        logs (verbose=False). Retry helps past GitHub's push-time workflow-file
        check timing out on a large repo.
        """
        repo_url = (
            "https://x-access-token:${token}@github.com/" + shlex.quote(repo) + ".git"
        )
        force_flag = "--force " if force else ""
        push_cmd = (
            'token="$(gh auth token)" && '
            "git -c http.https://github.com/.extraheader= push "
            f"{force_flag}{repo_url} {shlex.quote(refspec)}"
        )
        return Shell.check(
            push_cmd, dry_run=dry_run, strict=strict, verbose=False, retries=retries
        )

    def __init__(self):
        self.latest_tag = Shell.get_output("git describe --tags --abbrev=0") or ""
        self.new_tag = ""
        self.commits_since_latest = 0
        self.commits_since_new = 0
        if self.latest_tag:
            self.commits_since_latest = int(
                Shell.get_output(
                    f"git rev-list {self.latest_tag}..HEAD --first-parent --count"
                )
                or 0
            )
            if self.latest_tag.endswith("-new"):
                self.new_tag = Shell.get_output(
                    f"git describe --tags --abbrev=0 --exclude='{self.latest_tag}'"
                ) or ""
                if self.new_tag:
                    self.commits_since_new = int(
                        Shell.get_output(
                            f"git rev-list {self.new_tag}..HEAD --first-parent --count"
                        )
                        or 0
                    )

    def _tweak_for(self, tag: str, commits: int) -> int:
        if not tag.endswith("-testing"):
            if commits:
                return commits
            version_part = tag.split("-", 1)[0]
            try:
                return int(version_part.split(".")[-1])
            except ValueError:
                return 1
        version_part = tag.split("-", 1)[0]
        return int(version_part.split(".")[-1]) + commits

    @property
    def tweak(self) -> int:
        return self._tweak_for(self.latest_tag, self.commits_since_latest)

    @property
    def tweak_to_new(self) -> int:
        if self.new_tag:
            return self._tweak_for(self.new_tag, self.commits_since_new)
        return 1
