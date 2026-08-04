from praktika import Workflow

from ci.defs.altinity_jobs import AltinityArtifactConfigs, AltinityJobConfigs

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

FUNCTIONAL_TESTS_JOBS = [
    *JobConfigs.functional_tests_jobs,
    *AltinityJobConfigs.cas_functional_tests_jobs,
]

# Add long retention tags to subset of artifacts
clickhouse_binaries_with_tags = []
for artifact in ArtifactConfigs.clickhouse_binaries + ArtifactConfigs.clickhouse_stripped_binaries:
    if artifact.name in BINARIES_WITH_LONG_RETENTION:
        artifact = artifact.add_tags({"retention": "long"})
    clickhouse_binaries_with_tags.append(artifact)

workflow = Workflow.Config(
    name="MasterCI",
    event=Workflow.Event.DISPATCH,
    tags=["*"],
    inputs=[
        Workflow.Config.InputConfig(
            name="no_cache",
            description="Run without cache",
            is_required=False,
            input_type="boolean",
            default_value="false",
        ),
    ],
    jobs=[
        # *JobConfigs.tidy_build_arm_jobs,
        *JobConfigs.build_jobs,
        # *JobConfigs.build_llvm_coverage_job,
        JobConfigs.coverage_build_jobs[1],
        *JobConfigs.release_build_jobs,
        # *[ # NOTE (strtgbb): we don't run special build jobs
        #     job.set_dependency(
        #         REGULAR_BUILD_NAMES  # + [JobConfigs.tidy_build_arm_jobs[0].name]  # NOTE (strtgbb): we don't run tidy build jobs
        #     )
        #     for job in JobConfigs.special_build_jobs
        # ],
        *JobConfigs.unittest_jobs,
        # *JobConfigs.unittest_llvm_coverage_job,
        JobConfigs.docker_server,
        JobConfigs.docker_keeper,
        *JobConfigs.install_check_master_jobs,
        *AltinityJobConfigs.sign_release_jobs,
        AltinityJobConfigs.source_upload_job,
        *JobConfigs.compatibility_test_jobs,
        *[
            j
            for j in FUNCTIONAL_TESTS_JOBS
            if "coverage" not in j.name
        ],
        # *JobConfigs.functional_test_llvm_coverage_jobs,
        # *JobConfigs.functional_test_excluded_from_llvm_job,
        *JobConfigs.functional_tests_jobs_coverage,
        *JobConfigs.functional_tests_jobs_azure,
        *JobConfigs.integration_test_jobs_required,
        *JobConfigs.integration_test_jobs_non_required,
        # *JobConfigs.integration_test_llvm_coverage_jobs,
        # *JobConfigs.integration_test_excluded_from_llvm_job,
        *JobConfigs.stress_test_jobs,
        *JobConfigs.stress_test_azure_jobs,
        *JobConfigs.ast_fuzzer_jobs,
        *JobConfigs.buzz_fuzzer_jobs,
        # *JobConfigs.performance_comparison_with_master_head_jobs, # NOTE (strtgbb): fails due to GH secrets not being handled properly
        # *JobConfigs.performance_comparison_with_release_base_jobs,
        *JobConfigs.clickbench_master_jobs,
        JobConfigs.sqltest_master_job,
        JobConfigs.sqllogic_test_master_job,
        JobConfigs.sqlstorm_test_job,
        # JobConfigs.llvm_coverage_job,
    ],
    additional_jobs=[
        "GrypeScan",
        "Regression",
        "CIReport",
    ],
    artifacts=[
        *ArtifactConfigs.unittests_binaries,
        *clickhouse_binaries_with_tags,
        *ArtifactConfigs.clickhouse_binaries_gh,
        *ArtifactConfigs.clickhouse_debians,
        *ArtifactConfigs.clickhouse_rpms,
        *ArtifactConfigs.clickhouse_tgzs,
        *AltinityArtifactConfigs.signed_hashes,
        ArtifactConfigs.fuzzers,
        ArtifactConfigs.fuzzers_corpus,
        # *ArtifactConfigs.llvm_profdata_file,
        # ArtifactConfigs.llvm_coverage_info_file,
    ],
    dockers=DOCKERS,
    enable_dockers_manifest_merge=True,
    set_latest_for_docker_merged_manifest=True,
    secrets=SECRETS,
    enable_job_filtering_by_changes=False,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    enable_commit_status_on_failure=True,
    enable_slack_feed=False,
    pre_hooks=[
        '[ "$GITHUB_REF_TYPE" != "tag" ] || python3 ./tests/ci/version_helper.py --check-tag',
        # "python3 ./ci/jobs/scripts/workflow_hooks/store_data.py", # NOTE (carlosfelipeor): we don't use this in master CI
        "python3 ./ci/jobs/scripts/workflow_hooks/version_log.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/parse_ci_tags.py",
        # "python3 ./ci/jobs/scripts/workflow_hooks/merge_sync_pr.py", # NOTE (strtgbb): we don't do this
    ],
    workflow_filter_hooks=[should_skip_job],
    post_hooks=[],
)

WORKFLOWS = [
    workflow,
]
