from praktika import Workflow

from ci.jobs.scripts.workflow_hooks.trusted import can_be_trusted
from ci.workflows.defs import ARTIFACTS, BASE_BRANCH, SECRETS
from ci.workflows.job_configs import JobConfigs

workflow = Workflow.Config(
    name="PR",
    event=Workflow.Event.PULL_REQUEST,
    base_branches=[BASE_BRANCH],
    jobs=[
        JobConfigs.docker_build_arm,
        JobConfigs.docker_build_amd,
        JobConfigs.style_check,
        JobConfigs.docs_job,
        JobConfigs.fast_test,
        *JobConfigs.build_jobs,
        *JobConfigs.unittest_jobs,
        JobConfigs.docker_sever,
        JobConfigs.docker_keeper,
        *JobConfigs.install_check_jobs,
        *JobConfigs.compatibility_test_jobs,
        *JobConfigs.functional_tests_jobs_required,
        *JobConfigs.functional_tests_jobs_non_required,
        JobConfigs.bugfix_validation_job,
        *JobConfigs.stateless_tests_flaky_pr_jobs,
        *JobConfigs.integration_test_jobs_required,
        *JobConfigs.integration_test_jobs_non_required,
        *JobConfigs.integration_test_asan_flaky_pr_jobs,
        *JobConfigs.stress_test_jobs,
        *JobConfigs.upgrade_test_jobs,
        *JobConfigs.clickbench_jobs,
        *JobConfigs.ast_fuzzer_jobs,
        *JobConfigs.buzz_fuzzer_jobs,
        *JobConfigs.performance_comparison_jobs,
    ],
    artifacts=ARTIFACTS,
    # dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    enable_merge_ready_status=True,
    pre_hooks=[
        "python3 ./ci/jobs/scripts/workflow_hooks/pr_description.py",
        can_be_trusted,
        "python3 ./ci/jobs/scripts/workflow_hooks/docker_digests.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/version_log.py",
    ],
    post_hooks=[
        "python3 ./ci/jobs/scripts/workflow_hooks/feature_docs.py",
    ],
)

WORKFLOWS = [
    workflow,
]
