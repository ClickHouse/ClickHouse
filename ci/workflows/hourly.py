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
            command="python3 ./ci/jobs/fetch_flaky_tests_issues.py",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
        )
    ],
    secrets=SECRETS,
    enable_report=True,
    enable_cidb=False,
    cron_schedules=["0 */3 * * 1-5"],
)

WORKFLOWS = [
    workflow,
]
