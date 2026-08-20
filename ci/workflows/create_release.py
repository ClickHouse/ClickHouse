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

# PR checks: rehearse the release pipeline in --dry-run so a change to the
# release code is exercised before it can break a real release. Digest-gated to
# the release code (they run only when it changes) and run on the small PR pool,
# not amd-release-maker (the PR workflow's `pr-` label prefix cannot resolve it).
# No robot PAT: the only remote write is `git push --dry-run`, which negotiates
# with the default token. --skip-repo/--skip-docker drop package export, image
# builds, and (via skip-docker) the docker changelog generation, so the checks
# stay to the git/gh release logic.
_release_dry_run_digest = Job.CacheDigestConfig(
    include_paths=[
        "./.github/workflows/create_release.yml",
        "./ci/workflows/create_release.py",
        "./ci/jobs/release_job.py",
        "./ci/jobs/scripts/create_release.py",
        "./ci/jobs/scripts/clickhouse_version.py",
    ],
)

# "new" cuts from master; release_job.py makes master a local branch when the
# checkout is detached (the PR case) so its `checkout("master")` steps resolve.
release_dry_run_new_job = Job.Config(
    name="Release Dry Run (new)",
    runs_on=RunnerLabels.ARM_SMALL,
    command=(
        "PYTHONPATH=. python3 ./ci/jobs/release_job.py"
        " --ref master --release-type new --dry-run --skip-repo --skip-docker"
    ),
    # Mint a gh token via the PR lambda so `gh` reads and `git push --dry-run`
    # authenticate: PR job commands otherwise get no GitHub token.
    enable_gh_auth=True,
    digest_config=_release_dry_run_digest,
    timeout=1800,
)

# "patch" needs an unreleased release-branch commit; --ref auto finds one (a pass
# when none exists). --max-candidates widens the per-branch commit scan.
release_dry_run_patch_job = Job.Config(
    name="Release Dry Run (patch)",
    runs_on=RunnerLabels.ARM_SMALL,
    command=(
        "PYTHONPATH=. python3 ./ci/jobs/release_job.py"
        " --ref auto --release-type patch --dry-run --skip-repo --skip-docker"
        " --max-candidates 8"
    ),
    enable_gh_auth=True,
    digest_config=_release_dry_run_digest,
    timeout=1800,
)

PR_DRY_RUN_JOBS = [release_dry_run_new_job, release_dry_run_patch_job]

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
