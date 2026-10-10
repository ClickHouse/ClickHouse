"""ClickHouse new-release-branch pipeline job.

Cuts a new release branch off the given ref: pushes the release tag and the
branch, then bumps the version (on the branch and on master, opening the master
version-bump PR). Run by the `CreateReleaseBranch` workflow; the patch release is
the separate `release_job.py` / `CreateRelease` workflow. Each flow is a single
linear step sequence in its own file.

INVARIANT: every run starts in a clean, empty GitHub Actions `_work` directory -
the runner is ephemeral and the workspace is a fresh `actions/checkout` (a depth-1
shallow clone). There is NO state carried over from a previous run. So do not add
"in case a previous run left X on a reused runner" defenses here: there is no
reuse. The repo is always shallow at the start (hence the unconditional
`--unshallow`), and no leftover files/branches/credentials can exist.
"""

import os
import shlex
from pathlib import Path

from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.secret import Secret
from ci.praktika.utils import Shell, Utils
from ci.jobs.scripts.create_release import (
    ReleaseContextManager,
    ReleaseProgress,
)

_GH_TOKEN_SECRET = Secret.Config(
    name="/github-tokens/robot-1",
    type=Secret.Type.AWS_SSM_PARAMETER,
)

REPO_PATH = Utils.cwd()

RELEASE_INFO_FILE = "/tmp/release_info.json"


def main():
    stopwatch = Utils.Stopwatch()

    # Parameters come from the workflow inputs (workflow_dispatch / workflow_call),
    # read via praktika Info - there is no CLI.
    def _wi(name: str) -> str:
        value = Info.get_workflow_input_value(name)
        return "" if value is None else str(value)

    ref = _wi("ref")
    assert ref, "workflow input 'ref' must be set"
    dry_run = _wi("dry-run").lower() == "true"
    dry_run_flag = "--dry-run" if dry_run else ""

    # Drop a release-info file left by a previous release; "Prepare Release Info"
    # writes a fresh stub, so from here it exists only if that step ran this attempt.
    if os.path.exists(RELEASE_INFO_FILE):
        os.remove(RELEASE_INFO_FILE)

    original_branch = Shell.get_output("git rev-parse --abbrev-ref HEAD", strict=True)

    # Export the robot PAT (workflow scope) once; commands reference $GH_TOKEN so
    # praktika's verbose command logging never writes its value to the job log.
    os.environ["GH_TOKEN"] = _GH_TOKEN_SECRET.get_value()

    results = []
    ok = True

    def step(**kwargs):
        nonlocal ok
        if not ok:
            return
        results.append(Result.from_commands_run(**kwargs))
        if results[-1].status != Result.Status.OK:
            ok = False

    step(
        name="Fetch Repository History (treeless)",
        command=[
            # Treeless unshallow: every commit but no trees/blobs - history is all
            # the version tweak, changelog and contributors need, not its contents.
            "git fetch --quiet --filter=tree:0 --unshallow --no-recurse-submodules origin",
            # checkout fetches only the workflow ref; prepare needs
            # origin/<release_branch> and origin/master, so fetch all heads.
            "git fetch --quiet --no-recurse-submodules origin '+refs/heads/*:refs/remotes/origin/*'",
            "git fetch --quiet --tags --no-recurse-submodules origin",
        ],
        workdir=REPO_PATH,
    )

    step(
        name="Configure Git Auth for Release Pushes",
        command=[
            # Release pushes must use the robot token (not the checkout's
            # GITHUB_TOKEN extraheader) to carry the right permissions and trigger
            # ReleaseBranchCI.
            "git config --unset-all http.https://github.com/.extraheader || true",
            "gh auth setup-git",
        ],
        workdir=REPO_PATH,
    )

    step(
        name="Prepare Release Info",
        command=[
            f"python3 ./ci/jobs/scripts/create_release.py --prepare-release-info"
            f" --ref {shlex.quote(ref)} --release-type new"
            f" {dry_run_flag}".strip()
        ],
        workdir=REPO_PATH,
    )

    def _push_git_tag_for_release():
        with ReleaseContextManager(
            release_progress=ReleaseProgress.PUSH_RELEASE_TAG
        ) as release_info:
            release_info.push_release_tag(dry_run=dry_run)

    def _push_new_release_branch():
        with ReleaseContextManager(
            release_progress=ReleaseProgress.PUSH_NEW_RELEASE_BRANCH
        ) as release_info:
            release_info.push_new_release_branch(dry_run=dry_run)

    def _bump_version():
        with ReleaseContextManager(
            release_progress=ReleaseProgress.BUMP_VERSION
        ) as release_info:
            release_info.update_version_and_contributors_list(dry_run=dry_run)

    step(
        name="Push Git Tag for the Release",
        command=_push_git_tag_for_release,
        workdir=REPO_PATH,
    )

    step(
        name="Push New Release Branch",
        command=_push_new_release_branch,
        workdir=REPO_PATH,
    )

    # Bumps the branch and master versions and opens the master version-bump PR
    # in one call; idempotent, so a rerun self-skips a landed bump.
    step(
        name="Bump CH Version and Update Contributors' List",
        command=_bump_version,
        workdir=REPO_PATH,
    )

    # Always restore git state (Result.from_commands_run, not step(), so it runs
    # after a failure too).
    results.append(
        Result.from_commands_run(
            name="Checkout Back",
            command=[f"git checkout {original_branch}"],
            workdir=REPO_PATH,
        )
    )
    if results[-1].status != Result.Status.OK:
        ok = False

    # Post status only when prepare ran this attempt (else RELEASE_INFO_FILE is
    # absent and --post-status would raise FileNotFoundError).
    if os.path.exists(RELEASE_INFO_FILE):
        results.append(
            Result.from_commands_run(
                name="Post Slack Message",
                command=[
                    f"python3 ./ci/jobs/scripts/create_release.py --post-status"
                    f" {dry_run_flag}".strip()
                ],
                workdir=REPO_PATH,
            )
        )

    # Remove the env script holding the write-scoped release PAT so it does not
    # persist in ci/tmp for a later job on a reused runner.
    def cleanup_credentials():
        Path(REPO_PATH, "ci/tmp/praktika_setup_env.sh").unlink(missing_ok=True)

    results.append(
        Result.from_commands_run(
            name="Clean Up Credentials",
            command=cleanup_credentials,
            workdir=REPO_PATH,
        )
    )

    log_files = [RELEASE_INFO_FILE] if os.path.isfile(RELEASE_INFO_FILE) else []
    Result.create_from(
        results=results, stopwatch=stopwatch, files=log_files
    ).complete_job()


if __name__ == "__main__":
    main()
