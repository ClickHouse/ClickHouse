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

# PR check: rehearse the release pipeline in --dry-run on the small PR pool (no release-maker creds, no robot PAT — gh reads use the minted PR token); it publishes nothing but keeps every validation, so the recovery-ref guard still drives prepare() through its states.
_release_dry_run_digest = Job.CacheDigestConfig(
    include_paths=[
        "./.github/workflows/create_release.yml",
        "./ci/workflows/create_release.py",
        "./ci/jobs/release_job.py",
        "./ci/jobs/scripts/create_release.py",
        "./ci/jobs/scripts/clickhouse_version.py",
        "./ci/jobs/scripts/expect_release_refusal.py",
        # Other release entrypoints the dry run invokes directly; a change must invalidate the cache or the guard would miss it.
        "./tests/ci/changelog.py",
        "./ci/jobs/scripts/artifactory.py",
        "./ci/jobs/scripts/release_packages.py",
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


# "new" cuts from master (its vX.Y.1.1-new tag is the state prepare() expects); "patch" (--ref auto) rehearses a fresh patch and the artifact-download path; "recovery" (--ref recovery-auto) re-publishes a tagged release with --skip-repo --skip-docker, the one mode those flags are valid.
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

# Negative checks: prepare() must refuse these — "out of order" (a commit behind the branch's latest release) and "recovery misuse" (--skip-repo/--skip-docker on an untagged --ref auto); expect_release_refusal.py scores the refusal as a pass.
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
