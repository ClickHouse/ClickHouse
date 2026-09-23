from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS
from ci.defs.job_configs import JobConfigs

workflow = Workflow.Config(
    name="VectorSearchStress",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[JobConfigs.vector_search_stress_job],
    artifacts=[],
    secrets=SECRETS,
    dockers=DOCKERS,
    enable_report=True,
    enable_cidb=True,
    # enable_cache=True,
    cron_schedules=["47 15 * * 0"],  # Sunday 15:47
    pre_hooks=["python3 ./ci/jobs/scripts/workflow_hooks/store_data.py"],
)

WORKFLOWS = [
    workflow,
]
