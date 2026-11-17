from praktika import Job, Workflow

from ci.defs.defs import BASE_BRANCH, SECRETS, RunnerLabels

workflow = Workflow.Config(
    name="SyncSilk",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        Job.Config(
            name="Sync silk submodule",
            command="python3 ./ci/jobs/update_silk_submodule.py",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
            enable_gh_auth=True,
        ),
    ],
    secrets=SECRETS,
    enable_report=True,
    enable_cidb=False,
    cron_schedules=["17 */6 * * *"],
)

WORKFLOWS = [
    workflow,
]
