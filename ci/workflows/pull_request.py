from praktika import Workflow

from ci.workflows.defs import ARTIFACTS, BASE_BRANCH, DOCKERS, SECRETS, Jobs

S3_BUILDS_BUCKET = "clickhouse-builds"

workflow = Workflow.Config(
    name="PR",
    event=Workflow.Event.PULL_REQUEST,
    base_branches=[BASE_BRANCH],
    jobs=[
        Jobs.style_check_job.copy(),
        Jobs.fast_test_job,
        *Jobs.build_jobs,
        *Jobs.stateless_tests_jobs,
        *Jobs.stateful_tests_jobs,
        *Jobs.stress_test_jobs,
        *Jobs.upgrade_test_jobs,
        *Jobs.performance_comparison_head_jobs,
        *Jobs.compatibility_test_jobs,
        Jobs.docs_job,
        *Jobs.clickbench_jobs,
        Jobs.docker_job,
        Jobs.sqltest_job,
        Jobs.sqlancer_job,
        *Jobs.install_check_job,
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
