from praktika import Job, Secret, Workflow

from ci.defs.defs import BASE_BRANCH, SECRETS, RunnerLabels

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
# It runs on the cheap `arm_small` pool, NOT on `amd-release-maker`. It must not
# share a runner pool with CreateRelease: this job blocks in `gh run watch` for
# the whole batch, so if it held a release-maker slot it would starve the very
# CreateRelease runs it dispatched (which need that same scarce label) - a
# self-contention that stalled a dry run for hours. `arm_small` is abundant, so
# holding one slot while waiting is harmless. The artifact-readiness gate still
# lists packages in S3 via `S3Helper` (boto3), which resolves credentials from
# the runner's IAM instance role - available on `arm_small`, so no release-maker
# runner is needed just to read S3. The heavy release work runs in the dispatched
# CreateRelease runs on `amd-release-maker`.

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
    # Cheap, abundant pool: this job only reads GitHub + S3 and blocks watching
    # the CreateRelease runs it dispatches. Keep it off `amd-release-maker` so it
    # never competes with those runs for that scarce label (see the note above).
    runs_on=RunnerLabels.ARM_SMALL,
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
