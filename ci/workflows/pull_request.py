from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, SECRETS, ArtifactConfigs, JobNames
from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.workflow_hooks.should_skip_job import should_skip_job
from ci.jobs.scripts.workflow_hooks.trusted import can_be_trusted

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
        *[
            job.set_dependency([JobNames.STYLE_CHECK, JobNames.FAST_TEST])
            for job in JobConfigs.build_jobs
        ],
        *[
            job.set_dependency([JobNames.STYLE_CHECK, JobNames.FAST_TEST])
            for job in JobConfigs.special_build_jobs
        ],
        *JobConfigs.unittest_jobs,
        JobConfigs.docker_sever,
        JobConfigs.docker_keeper,
        *JobConfigs.install_check_jobs,
        *JobConfigs.compatibility_test_jobs,
        *JobConfigs.functional_tests_jobs_required,
        *JobConfigs.functional_tests_jobs_non_required,
        *JobConfigs.functional_tests_jobs_azure_master_only,
        JobConfigs.bugfix_validation_job,
        *JobConfigs.stateless_tests_flaky_pr_jobs,
        *JobConfigs.integration_test_jobs_required,
        *JobConfigs.integration_test_jobs_non_required,
        *JobConfigs.integration_test_asan_flaky_pr_jobs,
        *JobConfigs.stress_test_jobs,
        *JobConfigs.upgrade_test_jobs,
        *JobConfigs.ast_fuzzer_jobs,
        *JobConfigs.buzz_fuzzer_jobs,
        *JobConfigs.performance_comparison_amd_jobs,
        *JobConfigs.performance_comparison_arm_jobs,
    ],
    artifacts=[
        *ArtifactConfigs.unittests_binaries,
        *ArtifactConfigs.clickhouse_binaries,
        ArtifactConfigs.fast_test,
        *ArtifactConfigs.clickhouse_debians,
        *ArtifactConfigs.clickhouse_rpms,
        *ArtifactConfigs.clickhouse_tgzs,
        ArtifactConfigs.fuzzers,
        ArtifactConfigs.fuzzers_corpus,
        *ArtifactConfigs.performance_packages,
        *ArtifactConfigs.performance_reports,
    ],
    # dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    enable_merge_ready_status=True,
    enable_commit_status_on_failure=True,
    pre_hooks=[
        can_be_trusted,
        "python3 ./ci/jobs/scripts/workflow_hooks/store_data.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/pr_description.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/version_log.py",
    ],
    workflow_filter_hooks=[should_skip_job],
    post_hooks=[
        "python3 ./ci/jobs/scripts/workflow_hooks/feature_docs.py",
    ],
)

WORKFLOWS = [
    workflow,
]
