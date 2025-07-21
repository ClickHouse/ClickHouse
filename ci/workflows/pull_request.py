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
        *[
            job.set_dependency(
                [
                    JobNames.STYLE_CHECK,
                    JobNames.FAST_TEST,
                    *[j.name for j in JobConfigs.tidy_build_jobs],
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
            j for j in JobConfigs.functional_tests_jobs if "sequ" in j.name
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
