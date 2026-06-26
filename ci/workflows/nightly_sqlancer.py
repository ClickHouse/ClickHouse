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

# SQLancer runs against a debug ClickHouse server, so build the amd_debug binary
# in this workflow to satisfy the job's `CH_AMD_DEBUG` artifact requirement.
debug_build_job = Job.Config.get_job(
    JobConfigs.build_jobs, f"Build ({BuildTypes.AMD_DEBUG})"
).set_provides(ArtifactNames.CH_AMD_DEBUG, reset=True)

# TODO: add alert on workflow failure

workflow = Workflow.Config(
    name="NightlySQLancer",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        debug_build_job,
        *JobConfigs.sqlancer_master_jobs,
    ],
    artifacts=[
        *ArtifactConfigs.clickhouse_binaries,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    cron_schedules=["13 6 * * *"],
    pre_hooks=["python3 ./ci/jobs/scripts/workflow_hooks/store_data.py"],
)

WORKFLOWS = [
    workflow,
]
