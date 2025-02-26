from praktika import Job, Workflow

from ci.defs.defs import (
    BASE_BRANCH,
    SECRETS,
    ArtifactConfigs,
    ArtifactNames,
    BuildTypes,
    RunnerLabels,
)
from ci.defs.job_configs import JobConfigs

binary_build_job = (
    Job.Config.get_job(JobConfigs.build_jobs, f"Build ({BuildTypes.AMD_BINARY})")
    .set_dependency(JobConfigs.docker_build_amd.name, reset=True)
    .set_provides(ArtifactNames.CH_AMD_BINARY, reset=True)
)

jepsen_keeper_job = Job.Config(
    name="ClickHouse Keeper Jepsen",
    runs_on=RunnerLabels.STYLE_CHECK_ARM,
    command="cd ./tests/ci && python3 ci.py --run-from-praktika",
    requires=[binary_build_job.name],
)

# TODO: add alert on workflow failure

workflow = Workflow.Config(
    name="NightlyJepsen",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        # docker build jobs are just to ensure the docker images are ready,
        #   these jobs should be skipped in most of the cases
        JobConfigs.docker_build_arm,
        JobConfigs.docker_build_amd,
        binary_build_job,
        jepsen_keeper_job,
    ],
    artifacts=[
        *ArtifactConfigs.clickhouse_binaries,
    ],
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    cron_schedules=["13 4 * * *"],
    pre_hooks=["python3 ./ci/jobs/scripts/workflow_hooks/docker_digests.py"],
)

WORKFLOWS = [
    workflow,
]
