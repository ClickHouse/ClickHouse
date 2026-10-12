from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs
from ci.defs.job_configs import JobConfigs

workflow = Workflow.Config(
    name="OptimizeClickHouse",
    event=Workflow.Event.DISPATCH,
    branches=[BASE_BRANCH],
    jobs=[
        *JobConfigs.collect_clickhouse_profiles_jobs,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    artifacts=[
        ArtifactConfigs.clickhouse_pgo_profile_amd,
        ArtifactConfigs.clickhouse_pgo_profile_arm,
        ArtifactConfigs.clickhouse_bolt_profile_amd,
        ArtifactConfigs.clickhouse_bolt_profile_arm,
    ],
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
)

WORKFLOWS = [
    workflow,
]
