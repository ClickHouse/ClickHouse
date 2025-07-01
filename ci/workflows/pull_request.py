from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs, JobNames
from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.workflow_hooks.filter_job import should_skip_job
from ci.jobs.scripts.workflow_hooks.trusted import can_be_trusted

FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES = [
    job.name
    for job in JobConfigs.functional_tests_jobs
    if any(
        substr in job.name
        for substr in (
            "_debug, parallel",
            "_binary, parallel",
            "_asan, distributed plan, parallel",
        )
    )
]

REGULAR_BUILD_NAMES = [job.name for job in JobConfigs.build_jobs]

workflow = Workflow.Config(
    name="PR",
    event=Workflow.Event.PULL_REQUEST,
    base_branches=[BASE_BRANCH],
    jobs=[
        JobConfigs.style_check,
        JobConfigs.docs_job,
        JobConfigs.fast_test,
        *JobConfigs.tidy_build_jobs,
        *JobConfigs.tidy_arm_build_jobs,
        *[
            job.set_dependency(
                [
                    JobNames.STYLE_CHECK,
                    JobNames.FAST_TEST,
                    JobConfigs.tidy_build_jobs[0].name,
                ]
            )
            for job in JobConfigs.build_jobs
        ],
        *[
            job.set_dependency(REGULAR_BUILD_NAMES)
            for job in JobConfigs.special_build_jobs
        ],
        *JobConfigs.unittest_jobs,
        *[
            j.set_dependency(
                FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES if "sequential" in j.name else []
            )
            for j in JobConfigs.functional_tests_jobs
        ],
        JobConfigs.bugfix_validation_it_job.set_dependency(
            [
                JobNames.STYLE_CHECK,
                JobNames.FAST_TEST,
                JobConfigs.tidy_build_jobs[0].name,
            ]
        ),
        JobConfigs.bugfix_validation_ft_pr_job,
        *JobConfigs.stateless_tests_flaky_pr_jobs,
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.integration_test_jobs_required
        ],
        *[
            job.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES)
            for job in JobConfigs.integration_test_jobs_non_required
        ],
        JobConfigs.integration_test_asan_flaky_pr_job,
        JobConfigs.docker_sever.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES),
        JobConfigs.docker_keeper.set_dependency(FUNCTIONAL_TESTS_PARALLEL_BLOCKING_JOB_NAMES),
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
    secrets=SECRETS,
    enable_job_filtering_by_changes=True,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    enable_merge_ready_status=True,
    enable_gh_summary_comment=True,
    enable_commit_status_on_failure=True,
    pre_hooks=[
        can_be_trusted,
        "python3 ./ci/jobs/scripts/workflow_hooks/store_data.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/pr_description.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/version_log.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/quick_sync.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/team_notifications.py",
    ],
    workflow_filter_hooks=[should_skip_job],
    post_hooks=[
        "python3 ./ci/jobs/scripts/workflow_hooks/feature_docs.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/new_tests_check.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/can_be_merged.py",
    ],
)

WORKFLOWS = [
    workflow,
]
