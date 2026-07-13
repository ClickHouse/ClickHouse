from praktika import Job, Workflow

from ci.defs.defs import BASE_BRANCH, SECRETS, RunnerLabels

# TODO: add alert on workflow failure

# TODO: make it native praktika workflow + native praktika job - generated automatically if statistics feature is enabled for any workflow

workflow = Workflow.Config(
    name="NightlyStatistics",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    engine=Workflow.Engine.GH_ACTIONS,
    jobs=[
        Job.Config(
            name="Collect Statistics",
            command="python3 ./ci/jobs/collect_statistics.py",
            runs_on=RunnerLabels.ARM_TINY,
        )
    ],
    secrets=SECRETS,
    enable_report=True,
    enable_cidb=False,
    cron_schedules=["13 5 * * *"],
)

WORKFLOWS = [
    workflow,
]
