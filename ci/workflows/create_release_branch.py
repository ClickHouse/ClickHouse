from praktika import Job, Secret, Workflow

from ci.defs.defs import SECRETS

robot_token_secret = Secret.Config(
    name="ROBOT_CLICKHOUSE_COMMIT_TOKEN",
    type=Secret.Type.GH_SECRET,
)

release_branch_job = Job.Config(
    name="CreateReleaseBranch",
    runs_on=["self-hosted", "amd-release-maker"],
    command="PYTHONPATH=. python3 ./ci/jobs/release_branch_job.py",
    timeout=2 * 3600,
    # Push the release tag/branch/version-bump PR with the robot PAT (the App
    # token lacks the `workflow` scope). release_branch_job.py exports it as
    # GH_TOKEN.
    secrets=[robot_token_secret],
)

workflow = Workflow.Config(
    name="CreateReleaseBranch",
    event=Workflow.Event.DISPATCH,
    jobs=[release_branch_job],
    secrets=SECRETS + [robot_token_secret],
    # Cutting a branch mutates shared state (tag, branch, master bump PR); the
    # dispatch concurrency group serializes runs, and it must never overlap a
    # patch release either — the patch flow lives in the separate CreateRelease
    # workflow.
    enable_slack_feed=True,
    inputs=[
        Workflow.Config.InputConfig(
            name="ref",
            description="Git reference (branch or commit SHA) to cut the new release branch from",
            is_required=True,
            default_value="",
        ),
        Workflow.Config.InputConfig(
            name="dry-run",
            description="Dry run — show what would be done without making changes",
            is_required=False,
            default_value="false",
            is_boolean=True,
        ),
    ],
)

WORKFLOWS = [workflow]
