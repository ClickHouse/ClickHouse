"""ClickHouse new-release-branch pipeline job.

Cuts a new release branch off the given ref: pushes the release tag and the
branch, and opens the master version-bump PR (merged by the enqueue step). Run by
the `CreateReleaseBranch` workflow; the "patch" release is the separate
`release_job.py` / `CreateRelease` workflow. Each flow is a single linear step
sequence in its own file.

INVARIANT: every run starts in a clean, empty GitHub Actions `_work` directory -
the runner is ephemeral and the workspace is a fresh `actions/checkout` (a depth-1
shallow clone). There is NO state carried over from a previous run. So do not add
"in case a previous run left X on a reused runner" defenses here: there is no
reuse. The repo is always shallow at the start (hence the unconditional
`--unshallow`), and no leftover files/branches/credentials can exist.
"""

import json
import os
from pathlib import Path

from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.secret import Secret
from ci.praktika.utils import Shell, Utils

_GH_TOKEN_SECRET = Secret.Config(
    name="/github-tokens/robot-1",
    type=Secret.Type.AWS_SSM_PARAMETER,
)

REPO_PATH = Utils.cwd()

RELEASE_INFO_FILE = "/tmp/release_info.json"


def main():
    stopwatch = Utils.Stopwatch()

    # Parameters come from the workflow inputs (workflow_dispatch / workflow_call), read via praktika Info — there is no CLI.
    def _wi(name: str) -> str:
        value = Info.get_workflow_input_value(name)
        return "" if value is None else str(value)

    ref = _wi("ref")
    assert ref, "workflow input 'ref' must be set"
    dry_run = _wi("dry-run").lower() == "true"

    # Imported here (not at module top) so create_release's boto3 dependency is only pulled on the release machine, not at praktika config time.
    from ci.jobs.scripts import create_release

    # "Prepare Release Info" writes a fresh stub, so RELEASE_INFO_FILE below is present only if it ran this attempt.
    if os.path.exists(RELEASE_INFO_FILE):
        os.remove(RELEASE_INFO_FILE)

    original_branch = Shell.get_output("git rev-parse --abbrev-ref HEAD", strict=True)

    # Export the robot PAT (workflow scope) once; commands reference $GH_TOKEN so verbose logging never prints its value.
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
            # Treeless unshallow: every commit but no trees/blobs (the version tweak, changelog, and contributors need history only, not its file contents).
            "git fetch --quiet --filter=tree:0 --unshallow --no-recurse-submodules origin",
            # checkout fetches only the workflow ref; prepare needs origin/<release_branch> and origin/master, so fetch all heads.
            "git fetch --quiet --no-recurse-submodules origin '+refs/heads/*:refs/remotes/origin/*'",
            "git fetch --quiet --tags --no-recurse-submodules origin",
        ],
        workdir=REPO_PATH,
    )

    step(
        name="Configure Git Auth for Release Pushes",
        command=[
            # Release pushes must use the robot token (not the checkout's GITHUB_TOKEN extraheader) to carry the right permissions and trigger ReleaseBranchCI.
            "git config --unset-all http.https://github.com/.extraheader || true",
            "gh auth setup-git",
        ],
        workdir=REPO_PATH,
    )

    step(
        name="Prepare Release Info",
        command=create_release.prepare_release_info,
        command_kwargs=dict(ref=ref, release_type="new", dry_run=dry_run),
        workdir=REPO_PATH,
    )

    # This run creates the branch iff its master version-bump PR does not exist yet;
    # a rerun where it already exists (open) skips creation and only enqueues it.
    # `release_pr_absent` is that signal, and `release_pr_needs_merge` drives the merge.
    if dry_run:
        # No gh reads on dry-run: preview the full create-then-merge path.
        release_pr_absent = True
        release_pr_needs_merge = True
    else:
        release_pr_branch = None
        release_pr_state = ""  # "MERGED" | "OPEN" | ""
        if ok:
            with open(RELEASE_INFO_FILE) as f:
                _info = json.load(f)
            release_pr_branch = f"bump_version_{_info['version']}"
            release_pr_state = GH.get_pr_state_by_branch(
                release_pr_branch, "ClickHouse/ClickHouse"
            )
            print(
                f"Release PR branch [{release_pr_branch}] state: "
                + (release_pr_state or "absent — will create")
            )
        release_pr_absent = release_pr_branch is not None and release_pr_state == ""
        release_pr_needs_merge = (
            release_pr_branch is not None and release_pr_state != "MERGED"
        )

    # Cut the branch and open its master bump PR; the bump must precede the merge below.
    if release_pr_absent:
        step(
            name="Push Git Tag for the Release",
            command=create_release.push_release_tag,
            command_kwargs=dict(dry_run=dry_run),
            workdir=REPO_PATH,
        )

        step(
            name="Push New Release Branch",
            command=create_release.push_new_release_branch,
            command_kwargs=dict(dry_run=dry_run),
            workdir=REPO_PATH,
        )

        step(
            name="Bump CH Version and Update Contributors' List",
            command=create_release.create_bump_version_pr,
            command_kwargs=dict(dry_run=dry_run),
            workdir=REPO_PATH,
        )

    # Always restore git state (like `if: !cancelled()`); a failed restore must still block the merge below, so fold it into ok.
    results.append(
        Result.from_commands_run(
            name="Checkout Back",
            command=[f"git checkout {original_branch}"],
            workdir=REPO_PATH,
        )
    )
    if results[-1].status != Result.Status.OK:
        ok = False

    # Enqueue the master bump PR last (so its `CH Inc sync` check gets maximum time) and only when every preceding step succeeded; skipped if already merged.
    if release_pr_needs_merge:
        step(
            name="Update Release Info and Merge Created PRs",
            command=create_release.merge_prs,
            command_kwargs=dict(dry_run=dry_run),
            workdir=REPO_PATH,
        )

    # Post the final status only when prepare ran this attempt (else RELEASE_INFO_FILE is absent and post_status would raise FileNotFoundError).
    if os.path.exists(RELEASE_INFO_FILE):
        results.append(
            Result.from_commands_run(
                name="Post Slack Message",
                command=create_release.post_status,
                workdir=REPO_PATH,
            )
        )

    # Remove the env script holding the write-scoped release PAT so it does not persist in ci/tmp for a later job on a reused runner.
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
