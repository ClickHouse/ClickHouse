from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS
from ci.defs.job_configs import JobConfigs

workflow = Workflow.Config(
    name="NightlyKeeper",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[JobConfigs.keeper_stress_job],
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    cron_schedules=["17 3 * * *"],
    pre_hooks=["python3 ./ci/jobs/scripts/workflow_hooks/store_data.py"],
)

WORKFLOWS = [
    workflow,
]
