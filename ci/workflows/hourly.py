from praktika import Job, Workflow

from ci.defs.defs import BASE_BRANCH, SECRETS, RunnerLabels

# TODO: add alert on workflow failure

# TODO: make it native praktika workflow + native praktika job - generated automatically if statistics feature is enabled for any workflow

workflow = Workflow.Config(
    name="Hourly",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        Job.Config(
            name="Collect flaky tests",
            command="python3 ./ci/praktika/issue.py --collect-and-upload",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
            enable_gh_auth=True,
        ),
        Job.Config(
            name="Autoassign approvers",
            command="python3 ./ci/jobs/autoassign_approvers.py",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
            enable_gh_auth=True,
        ),
    ],
    secrets=SECRETS,
    enable_report=True,
    enable_cidb=False,
    cron_schedules=["0 */1 * * 1-5"],
)

WORKFLOWS = [
    workflow,
]
