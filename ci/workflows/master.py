from praktika import Workflow

from ci.defs.defs import (
    BASE_BRANCH,
    DOCKERS,
    GH_AUTH_TRUSTED_LAMBDA_NAME,
    LOOM_SECRETS,
    SECRETS,
    ArtifactConfigs,
    with_long_retention_tags,
)
from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.workflow_hooks.filter_job import should_skip_job
from ci.workflows.pull_request import REGULAR_BUILD_NAMES

# On master the plain (non-sanitizer) stateless suite runs against the optimized
# release binary instead of the plain `binary` build that PRs use. Drop the
# `arm_binary` jobs from `functional_tests_jobs` and add the `arm_release` and
# `amd_release` full-suite jobs. Master does not run the LLVM coverage jobs (the scheduled
# coverage workflow does), so the `amd_llvm_coverage` jobs of `functional_tests_jobs` are
# dropped too and the `DBReplicated`, `ParallelReplicas` and `AsyncInsert` configurations run
# on the `arm_binary` build, exactly as in pull requests (see `ci/workflows/pull_request.py`).
MASTER_FUNCTIONAL_TESTS_JOBS = (
    [
        job
        for job in JobConfigs.functional_tests_jobs
        if "arm_binary" not in job.name and "llvm_coverage" not in job.name
    ]
    + JobConfigs.functional_tests_master_release_jobs
    + JobConfigs.functional_tests_arm_binary_coverage_replacement_pr_jobs
)

clickhouse_binaries_with_tags = with_long_retention_tags(
    ArtifactConfigs.clickhouse_binaries
)

workflow = Workflow.Config(
    name="MasterCI",
    event=Workflow.Event.PUSH,
    branches=[BASE_BRANCH],
    engine=Workflow.Engine.GH_ACTIONS,
    jobs=[
        *JobConfigs.tidy_build_arm_jobs,
        *JobConfigs.build_jobs,
        *JobConfigs.release_build_jobs_with_examples,
        *JobConfigs.sccache_warmup_build_jobs,
        *[
            job.set_run_after(
                REGULAR_BUILD_NAMES + [j.name for j in JobConfigs.tidy_build_arm_jobs]
            )
            for job in JobConfigs.special_build_jobs
        ],
        *JobConfigs.wasm_parser_build_jobs,
        *JobConfigs.unittest_jobs,
        JobConfigs.docker_server,
        JobConfigs.docker_keeper,
        *JobConfigs.install_check_master_jobs,
        *JobConfigs.compatibility_test_jobs,
        *MASTER_FUNCTIONAL_TESTS_JOBS,
        *JobConfigs.functional_tests_jobs_azure,
        *JobConfigs.integration_test_jobs_required,
        *JobConfigs.integration_test_jobs_non_required,
        *JobConfigs.integration_test_arm_binary_coverage_replacement_pr_jobs,
        *JobConfigs.stress_test_jobs,
        *JobConfigs.stress_test_azure_jobs,
        *JobConfigs.ast_fuzzer_jobs,
        *JobConfigs.buzz_fuzzer_jobs,
        *JobConfigs.performance_comparison_with_master_head_jobs,
        *JobConfigs.clickbench_jobs,
        JobConfigs.sqltest_master_job,
        JobConfigs.sqllogic_test_master_job,
        JobConfigs.sqlstorm_test_job,
        JobConfigs.docs_examples_job,
    ],
    artifacts=[
        *ArtifactConfigs.unittests_binaries,
        *clickhouse_binaries_with_tags,
        *ArtifactConfigs.clickhouse_darwin_plain_binaries,
        *ArtifactConfigs.clickhouse_debians,
        *ArtifactConfigs.clickhouse_rpms,
        *ArtifactConfigs.clickhouse_tgzs,
        ArtifactConfigs.clickhouse_wasm,
        ArtifactConfigs.wasm_parser,
        ArtifactConfigs.fuzzers,
        ArtifactConfigs.fuzzers_corpus,
        ArtifactConfigs.clickhouse_examples,
    ],
    dockers=DOCKERS,
    enable_dockers_manifest_merge=True,
    set_latest_for_docker_merged_manifest=True,
    secrets=SECRETS + LOOM_SECRETS,
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
        "python3 ./ci/jobs/scripts/workflow_hooks/loom_code_refresh.py",
    ],
    workflow_filter_hooks=[should_skip_job],
    post_hooks=[],
    # merge_sync_pr.py needs a token with a broader permission scope - mint it
    # from the dedicated lambda.
    gh_auth_lambda_name=GH_AUTH_TRUSTED_LAMBDA_NAME,
)

WORKFLOWS = [
    workflow,
]
