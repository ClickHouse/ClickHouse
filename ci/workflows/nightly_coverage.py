from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs, ArtifactNames
from ci.defs.job_configs import JobConfigs

# TODO: add alert on workflow failure

workflow = Workflow.Config(
    name="NightlyCoverage",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        JobConfigs.coverage_build_jobs[0],
        *JobConfigs.functional_tests_jobs_coverage,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    artifacts=[
        *ArtifactConfigs.clickhouse_binaries,
    ],
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    cron_schedules=["13 2 * * *"],
)

WORKFLOWS = [
    workflow,
]
