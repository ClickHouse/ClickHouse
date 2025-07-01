from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs
from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.workflow_hooks.filter_job import should_skip_job
from ci.workflows.pull_request import REGULAR_BUILD_NAMES

workflow = Workflow.Config(
    name="MasterCI",
    event=Workflow.Event.PUSH,
    branches=[BASE_BRANCH],
    jobs=[
        *JobConfigs.tidy_build_jobs,
        *JobConfigs.build_jobs,
        *[
            job.set_dependency(
                REGULAR_BUILD_NAMES + [JobConfigs.tidy_build_jobs[0].name]
            )
            for job in JobConfigs.special_build_jobs
        ],
        *JobConfigs.unittest_jobs,
        JobConfigs.docker_sever,
        JobConfigs.docker_keeper,
        *JobConfigs.install_check_jobs,
        *JobConfigs.compatibility_test_jobs,
        *JobConfigs.functional_tests_jobs,
        *JobConfigs.functional_tests_jobs_azure_master_only,
        *JobConfigs.integration_test_jobs_required,
        *JobConfigs.integration_test_jobs_non_required,
        *JobConfigs.functional_tests_jobs_coverage,
        *JobConfigs.stress_test_jobs,
        *JobConfigs.stress_test_azure_master_jobs,
        *JobConfigs.ast_fuzzer_jobs,
        *JobConfigs.buzz_fuzzer_jobs,
        *JobConfigs.performance_comparison_with_master_head_jobs,
        *JobConfigs.clickbench_master_jobs,
        *JobConfigs.sqlancer_master_jobs,
        JobConfigs.sqltest_master_job,
    ],
    artifacts=[
        *ArtifactConfigs.unittests_binaries,
        *ArtifactConfigs.clickhouse_binaries,
        *ArtifactConfigs.clickhouse_debians,
        *ArtifactConfigs.clickhouse_rpms,
        *ArtifactConfigs.clickhouse_tgzs,
        ArtifactConfigs.fuzzers,
        ArtifactConfigs.fuzzers_corpus,
        *ArtifactConfigs.performance_reports,
    ],
    dockers=DOCKERS,
    enable_dockers_manifest_merge=True,
    set_latest_for_docker_merged_manifest=True,
    secrets=SECRETS,
    enable_job_filtering_by_changes=True,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    enable_commit_status_on_failure=True,
    pre_hooks=[
        "python3 ./ci/jobs/scripts/workflow_hooks/store_data.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/version_log.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/merge_sync_pr.py",
    ],
    workflow_filter_hooks=[should_skip_job],
    post_hooks=[],
)

WORKFLOWS = [
    workflow,
]
