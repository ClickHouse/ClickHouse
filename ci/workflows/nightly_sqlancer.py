from praktika import Job, Secret, Workflow

from ci.defs.defs import (
    BASE_BRANCH,
    DOCKERS,
    SECRETS,
    ArtifactConfigs,
    ArtifactNames,
    BuildTypes,
)
from ci.defs.job_configs import JobConfigs

# Both fuzzers run against two builds, so this workflow builds both:
#   * arm_release      - the wrong-result hunt. A sanitizer binary executes SQL
#                        2-3x slower, so a release build explores several times
#                        more generated SQL per hour.
#   * arm_asan_ubsan   - memory errors and UB reached through generated SQL.
release_build_job = Job.Config.get_job(
    JobConfigs.release_build_jobs, f"Build ({BuildTypes.ARM_RELEASE})"
).set_provides(ArtifactNames.CH_ARM_RELEASE, reset=True)
asan_ubsan_build_job = Job.Config.get_job(
    JobConfigs.build_jobs, f"Build ({BuildTypes.ARM_ASAN_UBSAN})"
).set_provides(ArtifactNames.CH_ARM_ASAN_UBSAN, reset=True)

workflow = Workflow.Config(
    name="NightlySQLancer",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        release_build_job,
        asan_ubsan_build_job,
        *JobConfigs.sqlancer_master_jobs,
        *JobConfigs.sqlancer_pp_jobs,
    ],
    artifacts=[
        *ArtifactConfigs.clickhouse_binaries,
    ],
    dockers=DOCKERS,
    # `SLACK_WEBHOOK_CORE_QA` (same webhook as NightlySchemaReplay in
    # clickhouse-private) is used by ci/jobs/scripts/sqlancer_notify.py to alert
    # #core-team-qa-alerts about failures this job has never reported before. The
    # job runs fine without it - the notifier just prints what it would have sent.
    secrets=[
        *SECRETS,
        Secret.Config(
            name="SLACK_WEBHOOK_CORE_QA",
            type=Secret.Type.GH_SECRET,
        ),
    ],
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    # Every 3 days (the runs themselves are ~5h); day-of-month step.
    cron_schedules=["13 6 */3 * *"],
    pre_hooks=["python3 ./ci/jobs/scripts/workflow_hooks/store_data.py"],
)

WORKFLOWS = [
    workflow,
]
