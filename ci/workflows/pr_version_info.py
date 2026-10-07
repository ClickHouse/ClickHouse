from praktika import Job, Workflow

from ci.defs.defs import BASE_BRANCH, SECRETS, RunnerLabels

# Maintains a "Version info" section in merged PR descriptions, sourcing the
# release version a PR shipped in from the CIDB `version_history` table. See
# tests/ci/pr_version_info.py for details.
#
# Runs every 30 minutes with a 10-day lookback: each run reconciles PRs merged in
# the last 10 days, plus any original pulled in by a backport merged in that
# window. A manual run can widen the lookback via the `days` input (e.g. 100 to
# backfill). The default `concurrency` group serializes runs: an in-progress run
# is left to finish and an intersecting newer run waits, so runs never overlap.

workflow = Workflow.Config(
    name="PRVersionInfo",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    inputs=[
        Workflow.Config.InputConfig(
            name="days",
            description="Lookback window in days; overrides the default for a "
            "manual run (e.g. 100 to backfill). Leave empty for the scheduled "
            "10-day window.",
            is_required=False,
            default_value="",
        ),
    ],
    jobs=[
        Job.Config(
            name="Update PR version info",
            command="python3 ./tests/ci/pr_version_info.py --days 10",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
            enable_gh_auth=True,
        ),
    ],
    secrets=SECRETS,
    enable_report=True,
    enable_cidb=False,
    # `0,30 * * * *` (minutes 0 and 30) rather than `*/30 * * * *`: praktika
    # emits the cron unquoted, and a YAML plain scalar may not start with `*`.
    cron_schedules=["0,30 * * * *"],
)

WORKFLOWS = [
    workflow,
]
