import json
import os
import shlex
import time
from typing import Dict, List, Optional, Tuple

from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.secret import Secret
from ci.praktika.utils import Shell, Utils

# Default branch releases are cut from, and the ref the dispatched CreateRelease
# runs use. Defined locally (not imported from ci.defs) so the job keeps its
# runtime PYTHONPATH minimal — importing ci.defs pulls in `from praktika import`,
# which the job's `PYTHONPATH=.` command does not resolve (mirrors release_job.py).
MAIN_BRANCH = "master"

# Only inspect the last few commits on each release branch for a green release
# candidate. A branch that has fallen further behind than this is surfaced as
# not-ready rather than reaching arbitrarily deep into history.
MAX_COMMITS_TO_CONSIDER = 8

# The dispatched release workflow, referenced by its generated YAML file name
# (what `gh workflow run` / `gh run list --workflow` expect — the workflow *name*
# "CreateRelease" is not accepted with a `.yml` suffix). Generated from
# ci/workflows/create_release.py.
CREATE_RELEASE_WORKFLOW = "create_release.yml"

# How long to wait for the dispatched CreateRelease run to appear before giving
# up on identifying it (`gh workflow run` does not return the run id).
DISPATCH_DISCOVERY_TIMEOUT_S = 300
DISPATCH_DISCOVERY_INTERVAL_S = 10

# Push release dispatches with the robot PAT: `gh workflow run` needs a token
# with the `actions:write` scope, and the same token drives the read queries.
# Injected as a GH secret by the workflow (see ci/workflows/auto_releases.py).
_GH_TOKEN_SECRET = Secret.Config(
    name="ROBOT_CLICKHOUSE_COMMIT_TOKEN",
    type=Secret.Type.GH_SECRET,
)


def _fetch_history() -> None:
    """Make every release branch head and tag available locally.

    actions/checkout fetches only the workflow ref, but prepare reads
    `origin/<release_branch>` and the release tags to measure how far each
    branch has moved since its last release. Mirrors the fetch phase of
    ci/jobs/release_job.py."""
    Shell.check(
        "git fetch --unshallow --no-recurse-submodules origin ||:",
        verbose=True,
    )
    Shell.check(
        "git fetch --no-recurse-submodules origin '+refs/heads/*:refs/remotes/origin/*'",
        strict=True,
        verbose=True,
    )
    Shell.check(
        "git fetch --tags --no-recurse-submodules origin", strict=True, verbose=True
    )


def _release_branches() -> List[str]:
    """Head branches of the open `release`-labeled PRs, oldest first.

    A persistent read failure raises rather than returning an empty list: a
    silent empty result would make an autorelease run quietly release nothing
    (fail-close). A successful `--json` read always prints at least `[]`."""
    raw = GH.get_output_with_retries(
        "gh pr list --state open --label release --json headRefName"
    )
    if not raw:
        raise RuntimeError("gh pr list failed for release PRs after retries")
    branches = sorted(pr["headRefName"] for pr in json.loads(raw))
    print(f"Found release branches {branches}")
    return branches


def _assert_no_open_version_bump_prs() -> None:
    """Refuse to release while a previous version-bump PR is still open.

    Each release opens a changelog PR titled `Update version_date.tsv and
    changelog after <tag>` (release_job.py); a lingering open one means the
    previous release did not finish merging, so releasing again would stack
    version bumps. Fail-close: a read failure raises too.

    The match is scoped with `in:title`: the legacy guard searched the phrase
    across all PR fields, so any unrelated PR merely mentioning
    `Update version_date.tsv` in its body (e.g. this migration's own PR) would
    trip it and halt every release. Restricting to the title keeps the guard
    firing on the real bump PRs while ignoring body-only mentions."""
    raw = GH.get_output_with_retries(
        'gh pr list --state open --search "Update version_date.tsv in:title"'
        " --json number,title"
    )
    if raw is None or raw == "":
        raise RuntimeError("gh pr list failed while checking for open version-bump PRs")
    prs = json.loads(raw)
    if prs:
        raise RuntimeError(f"Found not merged version bump PRs: {prs}")


def _latest_release_tag(branch: str) -> Optional[str]:
    """Newest `v<branch>.*` tag by version order, or None when there is none.

    Uses `--sort=v:refname` (numeric-aware) so v9 sorts before v10, unlike the
    lexical ref order the legacy PyGithub lookup relied on."""
    out = Shell.get_output(f"git tag -l {shlex.quote(f'v{branch}.*')} --sort=v:refname")
    tags = [t for t in out.splitlines() if t.strip()]
    return tags[-1] if tags else None


def _wf_completed(sha: str) -> bool:
    """True when every check run on the commit has completed.

    Empty check-runs means CI has not started reporting yet — treat as not
    completed (the commit is not a release candidate) rather than as done."""
    out = GH.get_output_with_retries(
        f"gh api --paginate repos/{{owner}}/{{repo}}/commits/{sha}/check-runs"
        f" --jq '.check_runs[].status'"
    )
    statuses = [s for s in out.splitlines() if s.strip()]
    if not statuses:
        print(f"   No check runs reported yet for [{sha}]")
        return False
    incomplete = [s for s in statuses if s != "completed"]
    if incomplete:
        print(f"   {len(incomplete)} check run(s) still in progress for [{sha}]")
        return False
    return True


def _failed_statuses(sha: str) -> List[str]:
    """Commit-status contexts whose latest state is not success.

    Keeps only the newest status per context (GitHub records one row per
    update) before deciding pass/fail, matching the legacy logic."""
    out = GH.get_output_with_retries(
        f"gh api --paginate repos/{{owner}}/{{repo}}/commits/{sha}/statuses"
        f" --jq '.[] | [.context, .state, .updated_at] | @tsv'"
    )
    latest: Dict[str, Tuple[str, str]] = {}
    for line in out.splitlines():
        if not line.strip():
            continue
        context, state, updated_at = line.split("\t")
        if context not in latest or latest[context][1] < updated_at:
            latest[context] = (state, updated_at)
    return [ctx for ctx, (state, _) in latest.items() if state != "success"]


def _find_release_candidate(branch: str) -> Tuple[str, str]:
    """Return (commit_sha, reason) for `branch`.

    commit_sha is the newest fully-green commit within MAX_COMMITS_TO_CONSIDER
    of the branch head, excluding the version-bump commit; empty when none
    qualifies, with `reason` explaining why."""
    tag = _latest_release_tag(branch)
    if not tag:
        return "", "no release tag found"
    if tag.endswith("new"):
        return "", f"new release branch (tag {tag}) - skip auto release"

    commits = Shell.get_output_or_raise(
        f"git rev-list --first-parent {shlex.quote(tag)}..origin/{shlex.quote(branch)}"
    ).splitlines()
    if not commits:
        return "", f"no new commits since {tag}"

    print(f"[{branch}]: {len(commits)} commit(s) since {tag}")
    # `git rev-list` lists newest first, so the oldest commit in the
    # `tag..branch` range — commits[-1] — is the version-bump commit pushed
    # right after `tag` was cut. Drop it (never a release candidate), then keep
    # the newest MAX_COMMITS_TO_CONSIDER of what remains.
    commits_to_check = commits[:-1][:MAX_COMMITS_TO_CONSIDER]
    last_failure = ""
    for idx, commit in enumerate(commits_to_check):
        print(f"[{branch}~{idx + 1}] check commit [{commit}] as release candidate")
        if not _wf_completed(commit):
            print("   CI in progress - check previous commit")
            continue
        failed = _failed_statuses(commit)
        if not failed:
            return commit, ""
        print(f"   CI failed: {failed} - check previous commit")
        last_failure = last_failure or f"failed jobs: {failed}"

    return "", last_failure or "no completed green commit in range"


def _latest_create_release_run_id() -> int:
    out = GH.get_output_with_retries(
        f"gh run list --workflow {CREATE_RELEASE_WORKFLOW} -L1 --json databaseId"
        f" --jq '.[0].databaseId // 0'"
    )
    return int(out.strip() or "0")


def _await_new_run(after_id: int) -> int:
    """Return the databaseId of the CreateRelease run created by our dispatch.

    `gh workflow run` does not report the run id, so poll the run list for the
    first workflow_dispatch run newer than the id captured before dispatch."""
    deadline = time.monotonic() + DISPATCH_DISCOVERY_TIMEOUT_S
    while time.monotonic() < deadline:
        out = GH.get_output_with_retries(
            f"gh run list --workflow {CREATE_RELEASE_WORKFLOW} -L10"
            f" --json databaseId,event"
        )
        runs = json.loads(out) if out else []
        newer = sorted(
            r["databaseId"]
            for r in runs
            if r["databaseId"] > after_id and r["event"] == "workflow_dispatch"
        )
        if newer:
            return newer[0]
        time.sleep(DISPATCH_DISCOVERY_INTERVAL_S)
    raise RuntimeError(
        f"CreateRelease run newer than [{after_id}] did not appear within"
        f" {DISPATCH_DISCOVERY_TIMEOUT_S}s"
    )


def _dispatch_and_wait(branch: str, sha: str, dry_run: bool) -> bool:
    """Dispatch CreateRelease for one branch and block until it finishes.

    Dispatches are serialized by waiting for each run to complete before the
    next one starts: CreateRelease's concurrency group keeps only the most
    recent pending run, so firing several at once would silently drop branches.
    Returns True when the run concluded successfully."""
    dry = "true" if dry_run else "false"
    before = _latest_create_release_run_id()
    print(f"Dispatch CreateRelease for [{branch}] at commit [{sha}] (dry-run={dry})")
    Shell.check(
        f"gh workflow run {CREATE_RELEASE_WORKFLOW} --ref {MAIN_BRANCH}"
        f" -f ref={shlex.quote(sha)} -f type=patch -f dry-run={dry}",
        strict=True,
        verbose=True,
    )
    run_id = _await_new_run(before)
    print(f"CreateRelease run [{run_id}] started; waiting for completion")
    # `gh run watch --exit-status` blocks until the run finishes and exits
    # non-zero if it concluded in failure.
    return Shell.check(
        f"gh run watch {run_id} --exit-status --interval 30", verbose=True
    )


def main() -> None:
    stopwatch = Utils.Stopwatch()

    os.environ["GH_TOKEN"] = _GH_TOKEN_SECRET.get_value()

    dry_run = (Info.get_workflow_input_value("dry-run") or "").lower() == "true"
    print(f"Auto release dry-run={dry_run}")

    _assert_no_open_version_bump_prs()
    _fetch_history()

    results: List[Result] = []
    for branch in _release_branches():
        print(f"\nChecking release branch [{branch}]")
        commit_sha, reason = _find_release_candidate(branch)
        if not commit_sha:
            print(f"[{branch}] not ready: {reason}")
            results.append(
                Result.create_from(
                    name=f"Release {branch}",
                    status=Result.Status.SKIPPED,
                    stopwatch=Utils.Stopwatch(),
                    info=f"not ready: {reason}",
                )
            )
            continue

        results.append(
            Result.from_commands_run(
                name=f"Release {branch} ({commit_sha[:12]})",
                command=lambda b=branch, s=commit_sha: _dispatch_and_wait(
                    b, s, dry_run
                ),
                fail_fast=False,
            )
        )

    Result.create_from(results=results, stopwatch=stopwatch).complete_job()


if __name__ == "__main__":
    main()
