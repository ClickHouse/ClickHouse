import re
import shlex
import time

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
        verbose: bool = True,
    ) -> bool:
        """Push `refspec` to `repo` over HTTPS with an App/PAT token.

        The token is `$GH_TOKEN` when the caller has one exported (the release
        job sets it to the robot PAT, which carries the `workflow` scope), else
        the App installation token from the `gh` session (native_jobs). It is
        used in the URL instead of the checkout's default GITHUB_TOKEN, and the
        inherited http extraheader is cleared per-command so that tokenized URL
        is what authenticates (only an App/PAT push re-triggers downstream
        workflows). The token expands at runtime, so its literal `${token}`
        stays out of the f-string and the URL is assembled by concatenation;
        `repo`/`refspec` are passed shell-quoted. Retry helps past GitHub's
        push-time workflow-file check timing out on a large repo.

        `verbose` is safe to enable: the command carries only the literal
        `${token}`/`$(gh auth token)` (expanded at runtime, and git redacts URL
        credentials), so the token never reaches the log while the push command
        and retry attempts stay visible.
        """
        # Log the files changed by the pushed commit, so it is visible whether
        # the push touches .github/workflows (the trigger for GitHub's
        # workflows-scope check).
        src_ref = refspec.split(":", 1)[0]
        commit = Shell.get_output(
            f"git rev-list -n1 {shlex.quote(src_ref)}", verbose=False
        )
        files = (
            Shell.get_output(
                f"git show --name-only --format= {shlex.quote(commit)}", verbose=False
            )
            if commit
            else ""
        )
        print(f"Files in pushed commit [{src_ref} -> {commit}]:\n{files or '(none)'}")

        repo_url = (
            "https://x-access-token:${token}@github.com/" + shlex.quote(repo) + ".git"
        )
        force_flag = "--force " if force else ""
        push_cmd = (
            'token="${GH_TOKEN:-$(gh auth token)}" && '
            "git -c http.https://github.com/.extraheader= push "
            f"{force_flag}{repo_url} {shlex.quote(refspec)}"
        )
        return Shell.check(
            push_cmd,
            dry_run=dry_run,
            strict=strict,
            verbose=verbose,
            retries=retries,
        )

    @staticmethod
    def push_tag(
        repo: str,
        tag: str,
        commit: str,
        message: str,
        user_name: str,
        user_email: str,
        dry_run: bool = False,
        retries: int = 1,
    ) -> None:
        """Create an annotated tag at `commit` and push it to `repo` as the App.

        Creates (force, so reruns are idempotent) the local annotated tag with
        the given tagger identity and no GPG signing, then pushes it with
        `Git.push` (App token) using the explicit `refs/tags/...` refspec.

        The local tag is created even on a dry run (only the push is skipped):
        later release steps resolve the tag locally (e.g. `changelog.py` runs
        `git rev-parse <tag>`), so a dry run that skipped the local tag would
        fail on a tag the real run would have created.
        """
        Shell.check(
            f"git -c user.name={shlex.quote(user_name)}"
            f" -c user.email={shlex.quote(user_email)} -c commit.gpgsign=false"
            f" tag -f -a -m {shlex.quote(message)}"
            f" {shlex.quote(tag)} {shlex.quote(commit)}",
            strict=True,
            verbose=True,
        )
        Git.push(
            repo,
            f"refs/tags/{tag}:refs/tags/{tag}",
            dry_run=dry_run,
            strict=True,
            retries=retries,
        )

    @staticmethod
    def merge_pull_request(
        pr_number, repo: str, retries: int = 12, delay: int = 5
    ) -> None:
        """Merge PR `pr_number` in `repo` directly with the merge-commit method.

        For a base branch with no merge queue (e.g. a release branch, unlike
        `master`): the PR cannot be enqueued, so it is merged directly. GitHub
        computes a PR's `mergeable` asynchronously after its head is pushed,
        reporting `UNKNOWN` until it settles, so a merge issued right after
        `gh pr create` can race that computation -- poll `mergeable` until it
        resolves, then merge. A genuine failure (conflicts, or the merge
        itself) is raised rather than swallowed, so a broken merge does not
        pass silently.
        """
        for attempt in range(retries):
            mergeable = (
                Shell.get_output(
                    f"gh pr view {pr_number} --repo {shlex.quote(repo)} "
                    "--json mergeable --jq .mergeable"
                )
                or ""
            ).strip()
            if mergeable == "MERGEABLE":
                break
            if mergeable == "CONFLICTING":
                raise RuntimeError(
                    f"PR #{pr_number} conflicts with the base branch in {repo} "
                    "and cannot be merged"
                )
            print(
                f"PR #{pr_number} mergeability not settled yet "
                f"(mergeable={mergeable or 'UNKNOWN'}); retrying "
                f"[{attempt + 1}/{retries}]"
            )
            time.sleep(delay)
        Shell.check(
            f"gh pr merge {pr_number} --repo {shlex.quote(repo)} --merge",
            strict=True,
            verbose=True,
        )

    def __init__(self):
        self.latest_tag = Shell.get_output("git describe --tags --abbrev=0") or ""
