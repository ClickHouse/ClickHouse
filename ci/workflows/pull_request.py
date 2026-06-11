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
            "_asan_ubsan, distributed plan, parallel",
            "_tsan, parallel",
        )
    )
]

STYLE_AND_FAST_TESTS = [
    JobNames.STYLE_CHECK,
    JobNames.FAST_TEST,
    JobNames.CI_TESTS,
    *[j.name for j in JobConfigs.tidy_build_arm_jobs],
]

REGULAR_BUILD_NAMES = [job.name for job in JobConfigs.build_jobs]

PLAIN_FUNCTIONAL_TEST_JOB = [
    j for j in JobConfigs.functional_tests_jobs if "amd_debug, parallel" in j.name
][0]

# NOTE: this PR workflow is temporarily trimmed down to a Darwin fast-test bed:
# 50 'Fast test (arm_darwin)' jobs in parallel, plus the arm_darwin build that
# provides their binary, and nothing else. This is for stress-testing the macOS
# fast-test lane (e.g. checking that disabling JIT reliably avoids the SIGILL),
# NOT intended to be merged. Each duplicate gets a unique name so the job digest
# (which includes the name) differs and all copies actually run instead of
# collapsing to a single cache hit. force_success was dropped from the job config
# so a failing run is visible at the top level instead of being forced green.
DARWIN_BUILD = [
    job for job in JobConfigs.special_build_jobs if job.name == "Build (arm_darwin)"
]

DARWIN_FAST_TESTS_PARALLEL = [
    JobConfigs.darwin_fast_test_jobs[0].set_name(f"Fast test (arm_darwin) [{i}]")
    for i in range(1, 51)
]

workflow = Workflow.Config(
    name="PR",
    event=Workflow.Event.PULL_REQUEST,
    base_branches=[BASE_BRANCH],
    jobs=[
        *DARWIN_BUILD,
        *DARWIN_FAST_TESTS_PARALLEL,
    ],
    artifacts=[
        *[
            a
            for a in ArtifactConfigs.clickhouse_binaries
            if a.name == ArtifactNames.CH_ARM_DARWIN_BIN
        ],
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
        "python3 ./ci/jobs/scripts/workflow_hooks/check_report_messages.py",
    ],
    job_aliases={},
    runs_on_label_prefix="pr-",
)

WORKFLOWS = [
    workflow,
]
