from praktika import Workflow

from ci.workflows.defs import ARTIFACTS, BASE_BRANCH, DOCKERS, SECRETS, Jobs

S3_BUILDS_BUCKET = "clickhouse-builds"

workflow = Workflow.Config(
    name="PR",
    event=Workflow.Event.PULL_REQUEST,
    base_branches=[BASE_BRANCH],
    jobs=[
        Jobs.style_check_job,
        Jobs.fast_test_job,
        *Jobs.build_jobs,
        *Jobs.stateless_tests_jobs,
        *Jobs.stateful_tests_jobs,
        *Jobs.stress_test_jobs,
        Jobs.performance_test_job,
        *Jobs.compatibility_test_jobs,
    ],
    artifacts=ARTIFACTS,
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_merge_ready_status=True,
)

WORKFLOWS = [
    workflow,
]
