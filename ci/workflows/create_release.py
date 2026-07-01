from praktika import Job, Secret, Workflow

from ci.defs.defs import SECRETS

robot_token_secret = Secret.Config(
    name="ROBOT_CLICKHOUSE_COMMIT_TOKEN",
    type=Secret.Type.GH_SECRET,
)

release_job = Job.Config(
    name="CreateRelease",
    runs_on=["self-hosted", "amd-release-maker"],
    command="PYTHONPATH=. python3 ./ci/jobs/release_job.py",
    timeout=2 * 3600,
    enable_gh_auth=True,
    secrets=[robot_token_secret],
)

workflow = Workflow.Config(
    name="CreateRelease",
    event=Workflow.Event.DISPATCH,
    jobs=[release_job],
    secrets=SECRETS + [robot_token_secret],
    # Releases mutate shared state (tags, package repos, Docker tags); never run
    # two concurrently. Mirrors the legacy workflow's `concurrency: group: release`.
    concurrency_group="release",
    # auto_releases.yml reuses this workflow via `uses:`, which requires a
    # `workflow_call` trigger in addition to `workflow_dispatch`.
    enable_workflow_call=True,
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
            name="only-repo",
            description="Run only repo updates including docker (repo-recovery, tests)",
            is_required=False,
            default_value="false",
            is_boolean=True,
        ),
        Workflow.Config.InputConfig(
            name="only-docker",
            description="Run only docker builds (repo-recovery, tests)",
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
