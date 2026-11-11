"""Nightly ClickHouse workflow for the ClickStack schema synchronization."""

from praktika import Job, Workflow

from ci.defs.defs import BASE_BRANCH, SECRETS, RunnerLabels


workflow = Workflow.Config(
    name="SyncClickStackSchemas",
    engine=Workflow.Engine.GH_ACTIONS,
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        Job.Config(
            name="Sync ClickStack schemas",
            command="python3 ./ci/jobs/sync_clickstack_schemas.py",
            runs_on=RunnerLabels.ARM_TINY,
            enable_gh_auth=True,
        ),
    ],
    secrets=SECRETS,
    enable_report=True,
    enable_cidb=False,
    cron_schedules=["17 8 * * *"],
)

WORKFLOWS = [
    workflow,
]
