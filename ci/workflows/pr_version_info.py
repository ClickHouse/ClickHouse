from praktika import Job, Workflow

from ci.defs.defs import BASE_BRANCH, SECRETS, RunnerLabels

# Maintains a "Version info" section in merged PR descriptions, sourcing the
# release version a PR shipped in from the CIDB `version_history` table. See
# tests/ci/pr_version_info.py for details.
#
# Runs hourly with a 1-day lookback: each run reconciles PRs merged in the last
# day, plus any original pulled in by a backport merged in that window.

workflow = Workflow.Config(
    name="PRVersionInfo",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        Job.Config(
            name="Update PR version info",
            command="python3 ./tests/ci/pr_version_info.py --days 1",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
            enable_gh_auth=True,
        ),
    ],
    secrets=SECRETS,
    enable_report=True,
    enable_cidb=False,
    cron_schedules=["0 * * * *"],
)

WORKFLOWS = [
    workflow,
]
