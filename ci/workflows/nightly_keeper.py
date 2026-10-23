from praktika import Job, Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs, ArtifactNames, BuildTypes
from ci.defs.job_configs import JobConfigs

binary_build_job = Job.Config.get_job(
    JobConfigs.build_jobs, f"Build ({BuildTypes.ARM_BINARY})"
).set_provides(ArtifactNames.CH_ARM_BINARY, reset=True)

# No-fault run: Sun / Tue / Thu / Sat
workflow = Workflow.Config(
    name="NightlyKeeperNoFaults",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        binary_build_job,
        JobConfigs.keeper_stress_job.set_name("Keeper Stress (No Faults)"),
    ],
    artifacts=[
        *ArtifactConfigs.clickhouse_binaries,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    cron_schedules=["17 1 * * 0,2,4,6"],
    pre_hooks=["python3 ./ci/jobs/scripts/workflow_hooks/store_data.py"],
)

WORKFLOWS = [workflow]
