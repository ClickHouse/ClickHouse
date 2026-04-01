from praktika import Workflow

from ci.defs.defs import (
    BASE_BRANCH,
    DOCKERS,
    SECRETS,
    ArtifactConfigs,
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
            # "_tsan, parallel",
        )
    )
]

STYLE_AND_FAST_TESTS = [
    # JobNames.STYLE_CHECK,
    JobNames.FAST_TEST,
    # *[j.name for j in JobConfigs.tidy_build_arm_jobs],
]

REGULAR_BUILD_NAMES = [job.name for job in JobConfigs.build_jobs]

PLAIN_FUNCTIONAL_TEST_JOB = [
    j for j in JobConfigs.functional_tests_jobs if "amd_debug, parallel" in j.name
][0]

workflow = Workflow.Config(
    name="PR",
    event=Workflow.Event.PULL_REQUEST,
    base_branches=[BASE_BRANCH, "releases/*", "antalya-*"],
    if_condition="github.repository == github.event.pull_request.head.repo.full_name || github.event_name == 'workflow_dispatch'",
    jobs=[
        # JobConfigs.style_check, # NOTE (strtgbb): we don't run style check
        # JobConfigs.docs_job, # NOTE (strtgbb): we don't build docs
        JobConfigs.fast_test,
        # *JobConfigs.tidy_build_arm_jobs, # NOTE (strtgbb): we don't run tidy build jobs
        *[job.set_dependency(STYLE_AND_FAST_TESTS) for job in JobConfigs.build_jobs],
        *[
            job.set_dependency(STYLE_AND_FAST_TESTS)
            for job in JobConfigs.extra_validation_build_jobs
        ],
        *[
            job.set_dependency(STYLE_AND_FAST_TESTS)
            for job in JobConfigs.release_build_jobs
        ],
        # *[
        #     job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
        #     for job in JobConfigs.special_build_jobs
        # ],
        *JobConfigs.build_llvm_coverage_job,
        # TODO: stabilize new jobs and remove set_allow_merge_on_failure
        JobConfigs.lightweight_functional_tests_job,
        JobConfigs.stateless_tests_targeted_pr_jobs[0].set_allow_merge_on_failure(),
        JobConfigs.integration_test_targeted_pr_jobs[0].set_allow_merge_on_failure(),
        JobConfigs.ast_fuzzer_targeted_pr_jobs[0].set_allow_merge_on_failure(),
        JobConfigs.ast_fuzzer_targeted_pr_jobs[1].set_allow_merge_on_failure(),
        # *JobConfigs.stateless_tests_flaky_pr_jobs,
        # *JobConfigs.integration_test_asan_flaky_pr_jobs,
        # JobConfigs.bugfix_validation_ft_pr_job,
        # JobConfigs.bugfix_validation_it_job,
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
        *JobConfigs.functional_test_llvm_coverage_jobs,
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.integration_test_jobs_required[:]
        ],
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.integration_test_jobs_non_required
        ],
        *JobConfigs.integration_test_llvm_coverage_jobs,
        *JobConfigs.unittest_jobs,
        *JobConfigs.unittest_llvm_coverage_job,
        JobConfigs.docker_server,
        JobConfigs.docker_keeper,
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
        # *[
        #     job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
        #     for job in JobConfigs.upgrade_test_jobs
        # ], # TODO: customize for our repo
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.ast_fuzzer_jobs
        ],
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.buzz_fuzzer_jobs
        ],
        # *[
        #    job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
        #    for job in JobConfigs.performance_comparison_with_master_head_jobs
        # ], # NOTE (strtgbb): failed previously due to GH secrets not being handled properly, try again later
        JobConfigs.llvm_coverage_job,
        JobConfigs.sqllogic_test_master_job.set_dependency(
            FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES
        ),
        *JobConfigs.toolchain_build_jobs,
        # TODO: uncomment when praktika supports depends-on-all-jobs;
        # currently set_dependency requires an explicit list, but CI Results Review
        # should only run after every other job has finished.
        # JobConfigs.ci_results_review.set_dependency(
        #     FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES
        # ),
    ],
    additional_jobs=["GrypeScan", "Regression", "CIReport", "SourceUpload"],
    artifacts=[
        *ArtifactConfigs.unittests_binaries,
        *ArtifactConfigs.clickhouse_binaries,
        *ArtifactConfigs.clickhouse_stripped_binaries,
        *ArtifactConfigs.clickhouse_debians,
        *ArtifactConfigs.clickhouse_rpms,
        *ArtifactConfigs.clickhouse_tgzs,
        ArtifactConfigs.fuzzers,
        ArtifactConfigs.fuzzers_corpus,
        ArtifactConfigs.parser_memory_profiler,
        *ArtifactConfigs.llvm_profdata_file,
        ArtifactConfigs.llvm_coverage_info_file,
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
    enable_merge_ready_status=False,  # NOTE (strtgbb): we don't use this, TODO, see if we can use it
    enable_gh_summary_comment=False,
    enable_commit_status_on_failure=True,
    enable_open_issues_check=False,
    enable_slack_feed=False,
    pre_hooks=[
        # can_be_trusted, # NOTE (strtgbb): relies on labels we don't use
        "python3 ./ci/jobs/scripts/workflow_hooks/store_data.py",
        # "python3 ./ci/jobs/scripts/workflow_hooks/pr_labels_and_category.py", # NOTE (strtgbb): relies on labels we don't use
        "python3 ./ci/jobs/scripts/workflow_hooks/version_log.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/parse_ci_tags.py",
        # "python3 ./ci/jobs/scripts/workflow_hooks/team_notifications.py",
    ],
    workflow_filter_hooks=[should_skip_job],
    post_hooks=[
        # "python3 ./ci/jobs/scripts/workflow_hooks/pr_body_check.py", # NOTE (strtgbb): Maybe we can use this
        # "python3 ./ci/jobs/scripts/workflow_hooks/feature_docs.py", # NOTE (strtgbb): we don't build docs
        # "python3 ./ci/jobs/scripts/workflow_hooks/new_tests_check.py", # NOTE (strtgbb): we don't use this
        # "python3 ./ci/jobs/scripts/workflow_hooks/can_be_merged.py", # NOTE (strtgbb): relies on labels we don't use
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
