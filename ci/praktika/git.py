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
    def enqueue_pull_request(
        pr: int,
        repo: str,
        dry_run: bool = False,
        verbose: bool = True,
        retries: int = 1,
        delay: int = 60,
    ) -> bool:
        """Add PR #`pr` in `repo` to the merge queue via `enqueuePullRequest`.

        `gh pr merge --auto` calls the `enablePullRequestAutoMerge` mutation,
        which the repo disables in favor of a merge queue on `master`; this calls
        `enqueuePullRequest` directly - the mutation the "Merge when ready" button
        on github.com uses.

        GitHub rejects `enqueuePullRequest` with "Pull request is already in the
        queue" when the PR is already queued (the "Merge when ready" button was
        clicked on github.com, or a previous run already enqueued it). That is the
        desired end state, so it is treated as success rather than a failure.

        A PR is only accepted once its required status checks have completed, so
        while a required check is still expected/pending the mutation is refused
        with UNPROCESSABLE. Retry up to `retries` times with
        `delay` seconds between attempts to wait that out; the same delay covers a
        transient `mergeStateStatus == UNKNOWN` right after a push.

        Returns whether the PR is in the queue. A persistent enqueue failure
        returns `False` (with the real `gh` output surfaced) rather than raising,
        so the caller decides how to react.
        """
        assert retries >= 1, "retries must be >= 1"
        if dry_run:
            print(f"Dry-run, would enqueue PR #{pr} in {repo} to the merge queue")
            return True

        pr_node_id = Shell.get_output(
            f"gh pr view {pr} --json id --jq '.id' --repo {shlex.quote(repo)}"
        ).strip()
        if not pr_node_id:
            print(f"ERROR: Failed to fetch node ID for PR #{pr}")
            return False

        enqueue_cmd = (
            "gh api graphql "
            "-f 'query=mutation($id:ID!){enqueuePullRequest(input:{pullRequestId:$id})"
            "{mergeQueueEntry{position state}}}' "
            f"-f id={shlex.quote(pr_node_id)}"
        )
        for attempt in range(1, retries + 1):
            returncode, stdout, stderr = Shell.get_res_stdout_stderr(
                enqueue_cmd, verbose=verbose
            )
            already_queued = "already in the queue" in (stdout + stderr).lower()
            if returncode == 0 or already_queued:
                break
            if attempt < retries:
                # A required check is likely still expected/pending, or GitHub is
                # still computing mergeability; wait and retry.
                reason = (stderr or stdout).strip().splitlines()
                print(
                    f"Enqueue attempt {attempt}/{retries} for PR #{pr} failed"
                    f"{' [' + reason[-1] + ']' if reason else ''};"
                    f" retrying in {delay}s"
                )
                time.sleep(delay)
        else:
            # Every attempt failed. Surface the real gh output so
            # auth/permission/validation/rate-limit failures stay diagnosable.
            if stdout:
                print(stdout)
            if stderr:
                print(stderr)
            print(
                f"ERROR: Failed to add PR #{pr} to the merge queue. "
                f"This often happens when mergeStateStatus is UNKNOWN "
                f"(GitHub is still computing mergeability after a recent push) "
                f"or the PR is not yet eligible (failing/pending required checks, "
                f"missing approvals, out of date with base). "
                f"Retry manually:\n  {enqueue_cmd}"
            )
            return False
        if already_queued:
            print(f"PR #{pr} is already in the merge queue")

        # Give GitHub a moment to update the PR's merge state, then verify it
        # actually landed in the queue.
        time.sleep(5)
        merge_status = Shell.get_output(
            f"gh pr view {pr} --json mergeStateStatus --jq '.mergeStateStatus'"
            f" --repo {shlex.quote(repo)}"
        )
        if merge_status == "QUEUED":
            print(f"OK: PR #{pr} added to the merge queue")
        else:
            print(
                f"WARNING: PR #{pr} enqueue mutation succeeded but "
                f"mergeStateStatus is {merge_status} (expected QUEUED). "
                f"Check the PR on github.com."
            )
        return True

    def __init__(self):
        self.latest_tag = Shell.get_output("git describe --tags --abbrev=0") or ""
