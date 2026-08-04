from praktika import Job, Secret, Workflow

from ci.defs.defs import BASE_BRANCH, SECRETS

# Scheduled patch-release driver (praktika port of the legacy
# .github/workflows/auto_releases.yml + tests/ci/auto_release.py).
#
# Once a day it scans each open `release`-labeled branch for the newest fully
# green commit whose release build artifacts are already in S3 and, for every
# branch that has one, dispatches the CreateRelease workflow
# (ci/workflows/create_release.py) for that commit. Releases are fired one
# branch at a time: the job waits for each CreateRelease run to finish before
# starting the next, because CreateRelease's concurrency group keeps only the
# most recent pending run and would otherwise silently drop a branch.
#
# It runs on the release-maker runner (same as CreateRelease): besides reading
# GitHub and dispatching/watching runs, its artifact-readiness gate lists the
# release packages in S3 through `S3Helper` (boto3 + credentials), which the
# cheap style-check runner does not have. The heavy release work still happens
# in the dispatched CreateRelease runs.

robot_token_secret = Secret.Config(
    name="ROBOT_CLICKHOUSE_COMMIT_TOKEN",
    type=Secret.Type.GH_SECRET,
)

# This one job awaits every ready branch's CreateRelease run serially (they
# cannot overlap: CreateRelease's concurrency group keeps only the most recent
# pending run and would drop the rest). So its timeout must cover the whole
# batch, not a single release. Size it as `max branches` * `CreateRelease's own
# per-branch cap` (2h, see ci/workflows/create_release.py) plus an hour of
# driver overhead (fetch, per-branch CI/artifact scans, dispatch discovery).
# The supported set (last 3 majors + latest LTS, per SECURITY.md) tops out
# around six open release branches; if it grows past MAX_RELEASE_BRANCHES this
# must grow with it, otherwise the last branches time out mid-batch.
CREATE_RELEASE_TIMEOUT_H = 2
MAX_RELEASE_BRANCHES = 6

auto_release_job = Job.Config(
    name="AutoReleaseInfo",
    # Same runner CreateRelease uses: the artifact-readiness gate needs S3
    # access (boto3 via S3Helper), unavailable on the style-check runner.
    runs_on=["self-hosted", "amd-release-maker"],
    command="PYTHONPATH=. python3 ./ci/jobs/auto_release_job.py",
    timeout=(CREATE_RELEASE_TIMEOUT_H * MAX_RELEASE_BRANCHES + 1) * 3600,
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
