from praktika import Job, Workflow

from ci.defs.defs import (
    BASE_BRANCH,
    DOCKERS,
    SECRETS,
    ArtifactConfigs,
    ArtifactNames,
    BuildTypes,
)
from ci.defs.job_configs import JobConfigs

# SQLancer runs against an ASan+UBSan ClickHouse server (the long run is a good
# place to surface memory errors and undefined behaviour), so build the
# arm_asan_ubsan binary in this workflow to satisfy the job's `CH_ARM_ASAN_UBSAN`
# artifact requirement.
asan_ubsan_build_job = Job.Config.get_job(
    JobConfigs.build_jobs, f"Build ({BuildTypes.ARM_ASAN_UBSAN})"
).set_provides(ArtifactNames.CH_ARM_ASAN_UBSAN, reset=True)

# TODO: add alert on workflow failure

workflow = Workflow.Config(
    name="NightlySQLancer",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        asan_ubsan_build_job,
        *JobConfigs.sqlancer_master_jobs,
        # SQLancer++ shares the same arm_asan_ubsan build (it also requires
        # CH_ARM_ASAN_UBSAN) and runs in parallel with SQLancer.
        *JobConfigs.sqlancer_pp_jobs,
    ],
    artifacts=[
        *ArtifactConfigs.clickhouse_binaries,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    # Every 3 days (the run itself is ~5h); day-of-month step.
    cron_schedules=["13 6 */3 * *"],
    pre_hooks=["python3 ./ci/jobs/scripts/workflow_hooks/store_data.py"],
)

WORKFLOWS = [
    workflow,
]
