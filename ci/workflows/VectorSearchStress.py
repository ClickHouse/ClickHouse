from praktika import Job, Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, RunnerLabels

vector_search_stress_job = Job.Config(
    name="Vector Search Stress",
    runs_on=RunnerLabels.ARM_SMALL,
    # run_in_docker="clickhouse/stateless-test+--shm-size=16g+--network=host",  # remove if run in docker is not required
    command="python3 ./ci/jobs/vector_search_stress_job.py",
)

workflow = Workflow.Config(
    name="VectorSearchStress",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[vector_search_stress_job],
    artifacts=[],
    secrets=SECRETS,
    # dockers=DOCKERS,
    enable_report=True,
    enable_cidb=True,
    # enable_cache=True,
    cron_schedules=["47 15 * * 0"],  # Sunday 15:47
    pre_hooks=["python3 ./ci/jobs/scripts/workflow_hooks/store_data.py"],
)

WORKFLOWS = [
    workflow,
]
