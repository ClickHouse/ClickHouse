from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs
from ci.defs.job_configs import JobConfigs

workflow = Workflow.Config(
    name="NightlyCoverage",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    engine=Workflow.Engine.GH_ACTIONS,
    jobs=[
        JobConfigs.coverage_build_jobs[
            1
        ],  # Per-test LLVM entry counters with randomized settings
        *JobConfigs.functional_tests_jobs_coverage,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    artifacts=ArtifactConfigs.clickhouse_binaries,
    enable_cache=True,
    enable_report=True,
    enable_slack_feed=True,
    enable_cidb=True,
    cron_schedules=["13 2 * * *"],
)

WORKFLOWS = [
    workflow,
]
