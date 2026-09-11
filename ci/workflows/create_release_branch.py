from praktika import Job, Secret, Workflow

from ci.defs.defs import SECRETS, RunnerLabels

robot_token_secret = Secret.Config(
    name="ROBOT_CLICKHOUSE_COMMIT_TOKEN",
    type=Secret.Type.GH_SECRET,
)

release_branch_job = Job.Config(
    name="CreateReleaseBranch",
    # A general-purpose runner: the new-branch flow only pushes the tag/branch and
    # opens the version-bump PR (git + gh + the SSM robot token); it needs none of
    # the release-maker's package/docker tooling.
    runs_on=RunnerLabels.ARM_SMALL,
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
    # Share one concurrency group with CreateRelease (and WeeklyOfficialDocker) so
    # a branch cut can never overlap an in-flight patch and publish a stale
    # `:latest` Docker tag for a superseded branch.
    concurrency_group="official-docker-library",
    # Cutting a branch mutates shared state (tag, branch, master bump PR); the
    # dispatch concurrency group serializes runs. It must never overlap a patch
    # release either - that flow lives in the separate CreateRelease workflow.
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
