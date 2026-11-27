from praktika import Job, Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs, ArtifactNames, BuildTypes
from ci.defs.job_configs import JobConfigs

binary_build_job = Job.Config.get_job(
    JobConfigs.build_jobs, f"Build ({BuildTypes.ARM_BINARY})"
).set_provides(ArtifactNames.CH_ARM_BINARY, reset=True)

# Fault run: Mon / Wed / Fri
workflow = Workflow.Config(
    name="NightlyKeeperFaults",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        binary_build_job,
        JobConfigs.keeper_stress_job.set_name("Keeper Stress (Faults)"),
    ],
    artifacts=[
        *ArtifactConfigs.clickhouse_binaries,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    cron_schedules=["17 1 * * 1,3,5"],
    pre_hooks=["python3 ./ci/jobs/scripts/workflow_hooks/store_data.py"],
)

WORKFLOWS = [workflow]
