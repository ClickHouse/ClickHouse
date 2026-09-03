from praktika import Job, Secret, Workflow

from ci.defs.defs import SECRETS, RunnerLabels

robot_token_secret = Secret.Config(
    name="ROBOT_CLICKHOUSE_COMMIT_TOKEN",
    type=Secret.Type.GH_SECRET,
)

release_job = Job.Config(
    name="CreateRelease",
    runs_on=["self-hosted", "amd-release-maker"],
    command="PYTHONPATH=. python3 ./ci/jobs/release_job.py",
    timeout=2 * 3600,
    # Push release tags/branches with the robot PAT (the App token lacks the
    # `workflow` scope, so GitHub's push-time workflow-scope check times out and
    # rejects tags whose `.github/workflows` differ from master — i.e. every
    # release-branch tag). release_job.py exports it as GH_TOKEN.
    secrets=[robot_token_secret],
)

# PR check: rehearse the release pipeline in --dry-run so a change to the release
# code is exercised before it can break a real release. Digest-gated to the
# release code (runs only when it changes) and on the small PR pool, not
# amd-release-maker (the PR workflow's `pr-` label prefix cannot resolve it). No
# robot PAT: a dry run pushes nothing and gh reads use the minted PR token
# (enable_gh_auth). A dry run never publishes and never skips a validation, so
# the recovery-ref guard stays active — the recovery/out-of-order/misuse cases
# below drive prepare() into each of its states. "new" cuts from master;
# release_job.py makes master a local branch when the checkout is detached (the
# PR case) so its checkout("master") resolves.
_release_dry_run_digest = Job.CacheDigestConfig(
    include_paths=[
        "./.github/workflows/create_release.yml",
        "./ci/workflows/create_release.py",
        "./ci/jobs/release_job.py",
        "./ci/jobs/scripts/create_release.py",
        "./ci/jobs/scripts/clickhouse_version.py",
        "./ci/jobs/scripts/expect_release_refusal.py",
    ],
)


def _dry_run_job(name: str, job_args: str) -> Job.Config:
    return Job.Config(
        name=name,
        runs_on=RunnerLabels.ARM_SMALL,
        command=f"PYTHONPATH=. python3 {job_args}",
        enable_gh_auth=True,
        digest_config=_release_dry_run_digest,
        timeout=1800,
    )


# "new" cuts a fresh release branch from master (release_job.py recreates a local
# master from origin on the detached PR checkout), whose version file and
# vX.Y.1.1-new tag are the state prepare() expects. "patch" (--ref auto) creates a
# new patch from the newest unreleased release-branch commit and, without
# --skip-repo, exercises the artifact-download path (which tolerates absent
# artifacts on a dry run). "recovery" re-publishes an already-tagged release
# (--ref recovery-auto resolves to a published tag) with --skip-repo --skip-docker,
# the one mode where those flags are valid.
_RELEASE_DRY_RUN_POSITIVE = [
    _dry_run_job(
        "Release Dry Run (new)",
        "./ci/jobs/release_job.py --ref master --release-type new --dry-run",
    ),
    _dry_run_job(
        "Release Dry Run (patch)",
        "./ci/jobs/release_job.py --ref auto --release-type patch --dry-run",
    ),
    _dry_run_job(
        "Release Dry Run (recovery)",
        "./ci/jobs/release_job.py --ref recovery-auto --release-type patch"
        " --dry-run --skip-repo --skip-docker",
    ),
]

# Negative checks: prepare() must *refuse* these. expect_release_refusal.py scores
# a refusal (non-zero exit carrying the expected message) as a pass. "out of
# order" targets a commit behind a branch's latest release; "recovery misuse"
# passes --skip-repo --skip-docker against an untagged (--ref auto) commit, which
# only a recovery may do, so the recovery-ref guard rejects it.
_RELEASE_DRY_RUN_NEGATIVE = [
    _dry_run_job(
        "Release Dry Run (out of order)",
        "./ci/jobs/scripts/expect_release_refusal.py"
        " --expect 'Refusing out-of-order release' --"
        " --ref out-of-order-auto --release-type patch"
        " --dry-run --skip-repo --skip-docker",
    ),
    _dry_run_job(
        "Release Dry Run (recovery misuse guard)",
        "./ci/jobs/scripts/expect_release_refusal.py"
        " --expect 'must be run against its release tag' --"
        " --ref auto --release-type patch"
        " --dry-run --skip-repo --skip-docker",
    ),
]

PR_DRY_RUN_JOBS = _RELEASE_DRY_RUN_POSITIVE + _RELEASE_DRY_RUN_NEGATIVE

workflow = Workflow.Config(
    name="CreateRelease",
    event=Workflow.Event.DISPATCH,
    jobs=[release_job],
    secrets=SECRETS + [robot_token_secret],
    # Releases mutate shared state (tags, package repos, Docker tags); never run
    # two concurrently. Dispatch workflows always emit `concurrency: group:
    # ${{ github.workflow }}`, which serializes CreateRelease runs. auto_releases.yml
    # reuses this workflow via `uses:`, relying on the `workflow_call` trigger that
    # dispatch workflows now always emit.
    # Route the job's pass/fail to the Slack Praktika app (the praktika-native
    # replacement for the dropped CIBuddy notifications), as master /
    # release_branches / pull_request do, so a failed release is not silent.
    enable_slack_feed=True,
    inputs=[
        Workflow.Config.InputConfig(
            name="ref",
            description="Git reference (branch or commit SHA) from which to create the release",
            is_required=True,
            default_value="",
        ),
        Workflow.Config.InputConfig(
            name="type",
            description="Release type - new for a new release branch, patch for a patch release",
            is_required=True,
            default_value="patch",
            options=["patch", "new"],
        ),
        Workflow.Config.InputConfig(
            name="skip-repo",
            description="Skip repo updates (package export/test); for recovery/rerun",
            is_required=False,
            default_value="false",
            is_boolean=True,
        ),
        Workflow.Config.InputConfig(
            name="skip-docker",
            description="Skip docker image builds; for recovery/rerun",
            is_required=False,
            default_value="false",
            is_boolean=True,
        ),
        Workflow.Config.InputConfig(
            name="dry-run",
            description="Dry run — show what would be done without making changes",
            is_required=False,
            default_value="false",
            is_boolean=True,
        ),
        Workflow.Config.InputConfig(
            name="assignee",
            description="GitHub login to assign the changelog PR to (optional)",
            is_required=False,
            default_value="",
        ),
    ],
)

WORKFLOWS = [workflow]
