from praktika import Workflow

from ci.defs.defs import (
    BASE_BRANCH,
    BINARIES_WITH_LONG_RETENTION,
    DOCKERS,
    SECRETS,
    ArtifactConfigs,
)
from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.workflow_hooks.filter_job import should_skip_job
from ci.workflows.pull_request import REGULAR_BUILD_NAMES

# Add long retention tags to subset of artifacts
clickhouse_binaries_with_tags = []
for artifact in ArtifactConfigs.clickhouse_binaries:
    if artifact.name in BINARIES_WITH_LONG_RETENTION:
        artifact = artifact.add_tags({"retention": "long"})
    clickhouse_binaries_with_tags.append(artifact)

workflow = Workflow.Config(
    name="MasterCI",
    event=Workflow.Event.PUSH,
    branches=[BASE_BRANCH],
    jobs=[
        *JobConfigs.tidy_build_arm_jobs,
        *JobConfigs.build_jobs,
        *JobConfigs.build_llvm_coverage_job,
        *JobConfigs.release_build_jobs,
        *[
            job.set_dependency(
                REGULAR_BUILD_NAMES + [JobConfigs.tidy_build_arm_jobs[0].name]
            )
            for job in JobConfigs.special_build_jobs
        ],
        JobConfigs.smoke_tests_macos,
        *JobConfigs.unittest_jobs,
        *JobConfigs.unittest_llvm_coverage_job,
        JobConfigs.docker_server,
        JobConfigs.docker_keeper,
        *JobConfigs.install_check_master_jobs,
        *JobConfigs.compatibility_test_jobs,
        *JobConfigs.functional_tests_jobs,
        *JobConfigs.functional_test_llvm_coverage_jobs,
        *JobConfigs.functional_tests_jobs_azure,
        *JobConfigs.integration_test_jobs_required,
        *JobConfigs.integration_test_jobs_non_required,
        *JobConfigs.integration_test_llvm_coverage_jobs,
        *JobConfigs.stress_test_jobs,
        *JobConfigs.stress_test_azure_jobs,
        *JobConfigs.ast_fuzzer_jobs,
        *JobConfigs.buzz_fuzzer_jobs,
        *JobConfigs.performance_comparison_with_master_head_jobs,
        *JobConfigs.performance_comparison_with_release_base_jobs,
        *JobConfigs.clickbench_master_jobs,
        # TODO: sqlancer needs adjustment after https://github.com/ClickHouse/ClickHouse/pull/81835
        #   job error: java.lang.AssertionError: CREATE TABLE IF NOT EXISTS database0NoREC.t1 (c0 String MATERIALIZED (-1457864079) CODEC (NONE)) ENGINE = MergeTree()  ORDER BY tuple()  SETTINGS allow_suspicious_indices=1;
        # *JobConfigs.sqlancer_master_jobs,
        JobConfigs.sqltest_master_job,
        JobConfigs.llvm_coverage_job,
    ],
    artifacts=[
        *ArtifactConfigs.unittests_binaries,
        *clickhouse_binaries_with_tags,
        *ArtifactConfigs.clickhouse_debians,
        *ArtifactConfigs.clickhouse_rpms,
        *ArtifactConfigs.clickhouse_tgzs,
        ArtifactConfigs.fuzzers,
        ArtifactConfigs.fuzzers_corpus,
        ArtifactConfigs.parser_memory_profiler,
        *ArtifactConfigs.llvm_profdata_file,
        ArtifactConfigs.llvm_coverage_html_report,
        ArtifactConfigs.llvm_coverage_info_file,
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
    enable_slack_feed=True,
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
