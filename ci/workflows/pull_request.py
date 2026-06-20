from praktika import Workflow

from ci.defs.defs import (
    BASE_BRANCH,
    DOCKERS,
    SECRETS,
    ArtifactConfigs,
    ArtifactNames,
    JobNames,
)
from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.workflow_hooks.filter_job import should_skip_job
from ci.jobs.scripts.workflow_hooks.trusted import can_be_tested

ALL_FUNCTIONAL_TESTS = [job.name for job in JobConfigs.functional_tests_jobs]

FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES = [
    job.name
    for job in JobConfigs.functional_tests_jobs
    if any(
        substr in job.name
        for substr in (
            "_debug, parallel",
            "_binary, parallel",
            "_asan, distributed plan, parallel",
            "_tsan, parallel",
        )
    )
]

STYLE_AND_FAST_TESTS = [
    JobNames.STYLE_CHECK,
    JobNames.FAST_TEST,
    *[j.name for j in JobConfigs.tidy_build_arm_jobs],
]

REGULAR_BUILD_NAMES = [job.name for job in JobConfigs.build_jobs]

PLAIN_FUNCTIONAL_TEST_JOB = [
    j for j in JobConfigs.functional_tests_jobs if "amd_debug, parallel" in j.name
][0]

workflow = Workflow.Config(
    name="PR",
    event=Workflow.Event.PULL_REQUEST,
    base_branches=[BASE_BRANCH],
    jobs=[
        JobConfigs.style_check,
        JobConfigs.docs_job,
        JobConfigs.fast_test,
        *JobConfigs.tidy_build_arm_jobs,
        *[job.set_dependency(STYLE_AND_FAST_TESTS) for job in JobConfigs.build_jobs],
        *[
            job.set_dependency(STYLE_AND_FAST_TESTS)
            for job in JobConfigs.extra_validation_build_jobs
        ],
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.release_build_jobs
        ],
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.special_build_jobs
        ],
        JobConfigs.smoke_tests_macos,
        # TODO: stabilize new jobs and remove set_allow_merge_on_failure
        JobConfigs.lightweight_functional_tests_job,
        JobConfigs.stateless_tests_targeted_pr_jobs[0].set_allow_merge_on_failure(),
        JobConfigs.integration_test_targeted_pr_jobs[0].set_allow_merge_on_failure(),
        *JobConfigs.stateless_tests_flaky_pr_jobs,
        *JobConfigs.integration_test_asan_flaky_pr_jobs,
        JobConfigs.bugfix_validation_ft_pr_job,
        JobConfigs.bugfix_validation_it_job,
        *[
            j.set_dependency(
                FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES
                if j.name not in FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES
                else []
            )
            for j in JobConfigs.functional_tests_jobs
        ],
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.functional_tests_jobs_azure
        ],
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.integration_test_jobs_required[:]
        ],
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.integration_test_jobs_non_required
        ],
        *JobConfigs.unittest_jobs,
        JobConfigs.docker_server.set_dependency(
            FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES
        ),
        JobConfigs.docker_keeper.set_dependency(
            FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES
        ),
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.install_check_jobs
        ],
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.compatibility_test_jobs
        ],
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.stress_test_jobs
        ],
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.upgrade_test_jobs
        ],
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.ast_fuzzer_jobs
        ],
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.buzz_fuzzer_jobs
        ],
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.performance_comparison_with_master_head_jobs
        ],
        *JobConfigs.toolchain_build_jobs,
    ],
    artifacts=[
        *ArtifactConfigs.unittests_binaries,
        *ArtifactConfigs.clickhouse_binaries,
        *ArtifactConfigs.clickhouse_debians,
        *ArtifactConfigs.clickhouse_rpms,
        *ArtifactConfigs.clickhouse_tgzs,
        ArtifactConfigs.fuzzers,
        ArtifactConfigs.fuzzers_corpus,
        ArtifactConfigs.parser_memory_profiler,
        ArtifactConfigs.toolchain_pgo_bolt_amd,
        ArtifactConfigs.toolchain_pgo_bolt_arm,
    ],
    dockers=DOCKERS,
    enable_dockers_manifest_merge=True,
    secrets=SECRETS,
    enable_job_filtering_by_changes=True,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    enable_merge_ready_status=True,
    enable_gh_summary_comment=True,
    enable_commit_status_on_failure=False,
    enable_open_issues_check=True,
    enable_slack_feed=True,
    pre_hooks=[
        can_be_tested,
        "python3 ./ci/jobs/scripts/workflow_hooks/store_data.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/pr_labels_and_category.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/version_log.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/team_notifications.py",
    ],
    workflow_filter_hooks=[should_skip_job],
    post_hooks=[
        "python3 ./ci/jobs/scripts/workflow_hooks/pr_body_check.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/feature_docs.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/new_tests_check.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/can_be_merged.py",
    ],
    job_aliases={
        "integration": JobConfigs.integration_test_jobs_non_required[
            0
        ].name,  # plain integration test job, no old analyzer, no dist plan
        "fast": "Fast test",
        "functional": PLAIN_FUNCTIONAL_TEST_JOB.name,
        "build_debug": "Build (amd_debug)",
        "build": "Build (amd_binary)",
    },
)

WORKFLOWS = [
    workflow,
]
