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
        Job.Config(
            # Investigates the failures that keep happening on master and
            # reverts the pull requests that caused them. See
            # ci/jobs/revert_ci_regressions.py. The job bounds itself to
            # RUN_BUDGET_SEC so that consecutive hourly runs never overlap;
            # the timeout here only catches a run that hangs past that.
            name="Revert CI regressions",
            command="python3 ./ci/jobs/revert_ci_regressions.py",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
            enable_gh_auth=True,
            # The job runs an AI agent over CI output that a merged pull
            # request can write, so the checkout must not carry the workflow
            # token in its git config: the agent would hold a GitHub
            # credential before any guard has run. The job fetches the public
            # repository anonymously and mints its own App token in the
            # revert path instead.
            checkout_persist_credentials=False,
            timeout=70 * 60,
        ),
    ],
    secrets=SECRETS,
    enable_report=True,
    enable_cidb=False,
    # Every hour, every day: a regression merged on a Friday evening must not
    # sit on master until Monday.
    cron_schedules=["0 * * * *"],
)

WORKFLOWS = [
    workflow,
]
