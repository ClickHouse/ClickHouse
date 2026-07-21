from praktika import Job, Secret, Workflow

from ci.defs.defs import BASE_BRANCH, SECRETS, RunnerLabels

# Scheduled patch-release driver (praktika port of the legacy
# .github/workflows/auto_releases.yml + tests/ci/auto_release.py).
#
# Once a day it scans each open `release`-labeled branch for the newest fully
# green commit and, for every branch that has one, dispatches the CreateRelease
# workflow (ci/workflows/create_release.py) for that commit. Releases are fired
# one branch at a time: the job waits for each CreateRelease run to finish
# before starting the next, because CreateRelease's concurrency group keeps only
# the most recent pending run and would otherwise silently drop a branch.
#
# The job only reads GitHub and dispatches/watches runs, so it runs on the cheap
# style-check runner rather than a release-maker; the heavy release work happens
# in the dispatched CreateRelease runs.

robot_token_secret = Secret.Config(
    name="ROBOT_CLICKHOUSE_COMMIT_TOKEN",
    type=Secret.Type.GH_SECRET,
)

auto_release_job = Job.Config(
    name="AutoReleaseInfo",
    runs_on=RunnerLabels.STYLE_CHECK_ARM,
    command="PYTHONPATH=. python3 ./ci/jobs/auto_release_job.py",
    # Sequential per-branch CreateRelease runs (up to ~2h each) are awaited in
    # this job, so allow for several release branches back to back.
    timeout=8 * 3600,
    enable_gh_auth=True,
    secrets=[robot_token_secret],
)

workflow = Workflow.Config(
    name="AutoReleases",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[auto_release_job],
    secrets=SECRETS + [robot_token_secret],
    # Route the job's pass/fail to the Slack Praktika app (the praktika-native
    # replacement for the dropped CIBuddy notifications), so a failed autorelease
    # is not silent. Each dispatched CreateRelease run reports its own status the
    # same way.
    enable_slack_feed=True,
    enable_report=True,
    enable_cidb=False,
    # 11:45 UTC daily, matching the legacy `45 11 * * *` schedule.
    cron_schedules=["45 11 * * *"],
    inputs=[
        Workflow.Config.InputConfig(
            name="dry-run",
            description="Dry run — dispatch CreateRelease with --dry-run so it "
            "makes no changes",
            is_required=False,
            default_value="false",
            is_boolean=True,
        ),
    ],
)

WORKFLOWS = [
    workflow,
]
