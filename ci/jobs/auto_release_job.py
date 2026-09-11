import json
import os
import shlex
import sys
import time
from typing import Dict, List, Optional, Tuple

from ci.jobs.scripts import release_packages
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.secret import Secret
from ci.praktika.utils import Shell, Utils

# `S3Helper` (boto3) lives under `tests/ci`; it lists the release artifacts in
# S3 for the artifact-readiness gate. Mirror `create_release.py`'s path setup so
# the gate talks to S3 through the exact same client CreateRelease uploads with.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../tests/ci"))
from s3_helper import S3Helper  # noqa: E402

# Default branch releases are cut from, and the ref the dispatched CreateRelease
# runs use. Defined locally (not imported from ci.defs) so the job keeps its
# runtime PYTHONPATH minimal — importing ci.defs pulls in `from praktika import`,
# which the job's `PYTHONPATH=.` command does not resolve (mirrors release_job.py).
MAIN_BRANCH = "master"

# Only inspect the last few commits on each release branch for a green release
# candidate. A branch that has fallen further behind than this is surfaced as
# not-ready rather than reaching arbitrarily deep into history.
MAX_COMMITS_TO_CONSIDER = 8

# Release-branch CI check-runs that validate the published artifacts install.
# AutoReleases gates a candidate on these instead of the whole ReleaseBranchCI,
# which also runs release-irrelevant test jobs (integration/stateless/stress)
# that are frequently red and would otherwise starve branches of patch releases.
# Names must match the `Install packages` jobs ReleaseBranchCI emits as check-runs.
RELEASE_VALIDATION_CHECKS = (
    "Install packages (amd_release)",
    "Install packages (arm_release)",
)

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
    # git fails `--unshallow` on an already complete repository.
    shallow = Shell.get_output_or_raise("git rev-parse --is-shallow-repository")
    if shallow.strip() == "true":
        Shell.check(
            "git fetch --unshallow --no-recurse-submodules origin",
            strict=True,
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


def _latest_release_tag(branch: str) -> Optional[str]:
    """Newest `v<branch>.*` tag by version order, or None when there is none.

    Uses `--sort=v:refname` (numeric-aware) so v9 sorts before v10, unlike the
    lexical ref order the legacy PyGithub lookup relied on."""
    out = Shell.get_output(f"git tag -l {shlex.quote(f'v{branch}.*')} --sort=v:refname")
    tags = [t for t in out.splitlines() if t.strip()]
    return tags[-1] if tags else None


def _release_checks_passed(sha: str) -> bool:
    """True when every RELEASE_VALIDATION_CHECKS check-run concluded success.

    Uses the newest run per name (GitHub keeps a row per re-run). A required
    check that is missing, unfinished, or not success makes the commit not a
    candidate."""
    out = GH.get_output_with_retries(
        f"gh api --paginate repos/{{owner}}/{{repo}}/commits/{sha}/check-runs"
        f" --jq '.check_runs[] | [.name, .started_at, .status, .conclusion] | @tsv'"
    )
    latest: Dict[str, Tuple[str, str, str]] = {}
    for line in out.splitlines():
        if not line.strip():
            continue
        name, started_at, status, conclusion = line.split("\t")
        if name not in latest or latest[name][0] < started_at:
            latest[name] = (started_at, status, conclusion)
    for name in RELEASE_VALIDATION_CHECKS:
        run = latest.get(name)
        if run is None:
            print(f"   {name} not reported yet for [{sha}]")
            return False
        _, status, conclusion = run
        if status != "completed":
            print(f"   {name} still {status} for [{sha}]")
            return False
        if conclusion != "success":
            print(f"   {name} concluded {conclusion} for [{sha}]")
            return False
    return True


def _release_version_for_commit(commit_sha: str) -> str:
    """The `major.minor.patch.tweak` version CreateRelease computes for a
    release-branch commit, derived here without checking it out.

    Matches `ci/jobs/scripts/clickhouse_version.py`
    (`CHVersion.get_current_version`): `major.minor.patch` come from the commit's
    `cmake/autogenerated_versions.txt`, and the tweak is the first-parent commit
    count since that file's `VERSION_GITHASH` (no tags), i.e. `git rev-list
    --count --first-parent <githash>..<commit>`."""
    content = Shell.get_output_or_raise(
        f"git show {commit_sha}:cmake/autogenerated_versions.txt"
    )
    parts: Dict[str, str] = {}
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("SET(VERSION_"):
            name, value = line[4:-1].split(maxsplit=1)
            parts[name] = value
    tweak = Shell.get_output_or_raise(
        f"git rev-list --count --first-parent {parts['VERSION_GITHASH']}..{commit_sha}"
    )
    return (
        f"{parts['VERSION_MAJOR']}.{parts['VERSION_MINOR']}."
        f"{parts['VERSION_PATCH']}.{tweak}"
    )


def _release_build_artifacts_ready(release_branch: str, commit_sha: str) -> bool:
    """Whether every object CreateRelease will download for this commit is
    already present in S3.

    A commit can pass the CI checks (`_wf_completed` + no failed statuses) while
    its release build was deduplicated by the CI cache — reported `skipped`,
    which is not a *failed* status — so nothing (or only part) was uploaded under
    this commit's SHA and CreateRelease would 404 downloading them. Delegate the
    exact per-object presence check to the shared `release_packages` module (the
    same contract CreateRelease's `PackageDownloader` produces), so this gate
    cannot drift from what CreateRelease actually downloads."""
    version = _release_version_for_commit(commit_sha)
    with_signed_macos = release_packages.commit_has_macos_signing(commit_sha)
    print(f"   signed macOS artifacts expected: {with_signed_macos}")
    return release_packages.release_build_artifacts_ready(
        S3Helper(), release_branch, commit_sha, version, with_signed_macos
    )


def _find_release_candidate(branch: str) -> Tuple[str, str, str]:
    """Return (commit_sha, reason, status) for `branch`.

    commit_sha is the newest commit within MAX_COMMITS_TO_CONSIDER of the branch
    head, excluding the version-bump commit, whose RELEASE_VALIDATION_CHECKS are
    green and whose release build artifacts are present; empty when none
    qualifies, with `reason` explaining why. `status` is what the sub-result
    reports: SKIPPED when not ready yet, ERROR when the branch is broken."""
    tag = _latest_release_tag(branch)
    if not tag:
        return "", "no release tag found", Result.Status.ERROR
    if tag.endswith("new"):
        return (
            "",
            f"new release branch (tag {tag}) - skip auto release",
            Result.Status.SKIPPED,
        )

    commits = Shell.get_output_or_raise(
        f"git rev-list --first-parent {shlex.quote(tag)}..origin/{shlex.quote(branch)}"
    ).splitlines()
    if not commits:
        return "", f"no new commits since {tag}", Result.Status.SKIPPED

    print(f"[{branch}]: {len(commits)} commit(s) since {tag}")
    # `git rev-list` lists newest first, so the oldest commit in the
    # `tag..branch` range — commits[-1] — is the version-bump commit pushed
    # right after `tag` was cut. Drop it (never a release candidate), then keep
    # the newest MAX_COMMITS_TO_CONSIDER of what remains.
    commits_to_check = commits[:-1][:MAX_COMMITS_TO_CONSIDER]
    last_failure = ""
    for idx, commit in enumerate(commits_to_check):
        print(f"[{branch}~{idx + 1}] check commit [{commit}] as release candidate")
        if not _release_checks_passed(commit):
            last_failure = last_failure or "release validation checks not green"
            continue
        # The install-packages checks prove the .deb/.rpm install, but not that
        # every object CreateRelease downloads is present: a dedup-skipped build
        # uploads nothing under this SHA (and macOS signed artifacts are separate),
        # so CreateRelease's package download 404s. Require them to be present.
        if not _release_build_artifacts_ready(branch, commit):
            print(
                f"   release validation green but release build artifacts missing"
                f" for [{commit}] - check previous commit"
            )
            last_failure = (
                last_failure
                or "release build artifacts missing (build skipped/cached)"
            )
            continue
        return commit, "", Result.Status.OK

    return (
        "",
        last_failure or "no commit with green release validation in range",
        Result.Status.SKIPPED,
    )


def _latest_create_release_run_id() -> int:
    """Newest CreateRelease run id, or 0 when the workflow has never run."""
    out = GH.get_output_with_retries(
        f"gh run list --workflow {CREATE_RELEASE_WORKFLOW} -L1 --json databaseId"
        f" --jq '.[0].databaseId // 0'",
        strict=True,
    )
    return int(out.strip())


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

    info = Info()
    assert info.repo_name == "ClickHouse/ClickHouse", f"got [{info.repo_name}]"
    _fetch_history()

    results: List[Result] = []
    for branch in _release_branches():
        print(f"\nChecking release branch [{branch}]")
        commit_sha, reason, status = _find_release_candidate(branch)
        if not commit_sha:
            print(f"[{branch}] not ready: {reason}")
            results.append(
                Result.create_from(
                    name=f"Release {branch}",
                    status=status,
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

    if not results:
        # No open release branches at all — a harmless empty set, not a
        # failure. `Result.create_from` defaults an empty result list to ERROR,
        # which would raise a daily false alarm, so report SKIPPED explicitly.
        print("No open release branches - nothing to release")
        Result.create_from(
            results=[],
            status=Result.Status.SKIPPED,
            info="no open release branches",
            stopwatch=stopwatch,
        ).complete_job()
        return

    Result.create_from(results=results, stopwatch=stopwatch).complete_job()


if __name__ == "__main__":
    main()
