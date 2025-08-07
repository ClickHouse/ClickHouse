from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs, JobNames
from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.workflow_hooks.filter_job import should_skip_job
from ci.jobs.scripts.workflow_hooks.trusted import can_be_trusted

REQUIRED_STATELESS_TESTS_JOB_NAMES = [
    job.name for job in JobConfigs.functional_tests_jobs_required if "asan" in job.name
]
REGULAR_BUILD_NAMES = [job.name for job in JobConfigs.build_jobs]

workflow = Workflow.Config(
    name="PR",
    event=Workflow.Event.PULL_REQUEST,
    base_branches=[BASE_BRANCH, "releases/*", "antalya-*"],
    jobs=[
        # JobConfigs.style_check, # NOTE (strtgbb): we don't run style check
        # JobConfigs.docs_job, # NOTE (strtgbb): we don't build docs
        # JobConfigs.fast_test, # NOTE (strtgbb): same fails as stateless tests, revisit later
        *JobConfigs.tidy_build_jobs,
        *JobConfigs.tidy_arm_build_jobs,
        *[
            job.set_dependency(
                [
                    # JobNames.STYLE_CHECK, # NOTE (strtgbb): we don't run style check
                    # JobNames.FAST_TEST, # NOTE (strtgbb): this takes too long, revisit later
                    # JobConfigs.tidy_build_jobs[0].name, # NOTE (strtgbb): this takes too long, revisit later
                ]
            )
            for job in JobConfigs.build_jobs
        ],
        # *[
        #     job.set_dependency(REGULAR_BUILD_NAMES)
        #     for job in JobConfigs.special_build_jobs
        # ],
        *JobConfigs.unittest_jobs,
        JobConfigs.docker_sever,
        JobConfigs.docker_keeper,
        *JobConfigs.install_check_jobs,
        *JobConfigs.compatibility_test_jobs,
        *JobConfigs.functional_tests_jobs_required,
        *JobConfigs.functional_tests_jobs_non_required,
        *[
            job.set_dependency(REQUIRED_STATELESS_TESTS_JOB_NAMES)
            for job in JobConfigs.functional_tests_jobs_coverage
        ],
        JobConfigs.bugfix_validation_it_job.set_dependency(
            [
                # JobNames.STYLE_CHECK, # NOTE (strtgbb): we don't run style check
                # JobNames.FAST_TEST, # NOTE (strtgbb): we don't run fast tests
                JobConfigs.tidy_build_jobs[0].name,
            ]
        ),
        JobConfigs.bugfix_validation_ft_pr_job,
        *JobConfigs.stateless_tests_flaky_pr_jobs,
        *JobConfigs.integration_test_jobs_required,
        *JobConfigs.integration_test_jobs_non_required,
        JobConfigs.integration_test_asan_flaky_pr_job,
        *JobConfigs.stress_test_jobs,
        # *JobConfigs.upgrade_test_jobs, # TODO: customize for our repo
        *JobConfigs.ast_fuzzer_jobs,
        *JobConfigs.buzz_fuzzer_jobs,
        # *JobConfigs.performance_comparison_with_master_head_jobs, # NOTE (strtgbb): fails due to GH secrets not being handled properly
    ],
    artifacts=[
        *ArtifactConfigs.unittests_binaries,
        *ArtifactConfigs.clickhouse_binaries,
        *ArtifactConfigs.clickhouse_debians,
        *ArtifactConfigs.clickhouse_rpms,
        *ArtifactConfigs.clickhouse_tgzs,
        ArtifactConfigs.fuzzers,
        ArtifactConfigs.fuzzers_corpus,
        # *ArtifactConfigs.performance_reports,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_job_filtering_by_changes=True,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    enable_merge_ready_status=False,  # NOTE (strtgbb): we don't use this, TODO, see if we can use it
    enable_commit_status_on_failure=True,
    pre_hooks=[
        # can_be_trusted, # NOTE (strtgbb): relies on labels we don't use
        "python3 ./ci/jobs/scripts/workflow_hooks/store_data.py",
        # "python3 ./ci/jobs/scripts/workflow_hooks/pr_description.py", # NOTE (strtgbb): relies on labels we don't use
        "python3 ./ci/jobs/scripts/workflow_hooks/version_log.py",
        # "python3 ./ci/jobs/scripts/workflow_hooks/quick_sync.py", # NOTE (strtgbb): we don't do this
        "python3 ./ci/jobs/scripts/workflow_hooks/new_tests_check.py",
    ],
    workflow_filter_hooks=[should_skip_job],
    post_hooks=[
        # "python3 ./ci/jobs/scripts/workflow_hooks/feature_docs.py", # NOTE (strtgbb): we don't build docs
        # "python3 ./ci/jobs/scripts/workflow_hooks/can_be_merged.py", # NOTE (strtgbb): relies on labels we don't use
    ],
)

WORKFLOWS = [
    workflow,
]
