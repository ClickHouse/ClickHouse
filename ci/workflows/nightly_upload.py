from praktika import Job, Workflow

from ci.defs.defs import BASE_BRANCH, SECRETS, RunnerLabels

workflow = Workflow.Config(
    name="NightlyUpload",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    engine=Workflow.Engine.GH_ACTIONS,
    jobs=[
        Job.Config(
            name="Upload clickhousectl",
            command="python3 ./ci/jobs/upload_clickhousectl.py",
            runs_on=RunnerLabels.ARM_TINY,
            enable_gh_auth=True,
        )
    ],
    secrets=SECRETS,
    enable_report=True,
    enable_cidb=False,
    cron_schedules=["13 6 * * *"],
)

WORKFLOWS = [
    workflow,
]
