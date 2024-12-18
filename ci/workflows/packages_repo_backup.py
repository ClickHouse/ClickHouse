from praktika import Workflow

from ci.workflows.defs import Jobs

nightly_workflow = Workflow.Config(
    name="PackagesRepoBakUp",
    event=Workflow.Event.SCHEDULE,
    jobs=[
        Jobs.style_check_job,
    ],
    cron_schedules=["13 3 * * *"],
)

WORKFLOWS = [
    nightly_workflow,
]
