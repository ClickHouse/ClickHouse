from praktika import Workflow

from ci.defs.defs import (
    BASE_BRANCH,
    DOCKERS,
    SECRETS,
    ArtifactConfigs,
    JobNames,
)
from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.workflow_hooks.filter_job import (
    PR_SINGLE_ARCH_SKIPPED_BUILDS,
    PR_SINGLE_ARCH_SKIPPED_JOBS,
    configure_amd_release_thinlto,
    configure_performance_shards,
    configure_toolchain_builds,
    should_skip_job,
)
from ci.jobs.scripts.workflow_hooks.trusted import can_be_tested

# PR sanitizer jobs repeat tests related to the change to find intermittent failures.
# Debug and plain binary jobs run the full suite. Master keeps the full suite in
# every configuration; coverage-based targeting is specific to pull requests.
FUNCTIONAL_TESTS_JOBS = JobConfigs.functional_tests_pr_jobs

ALL_FUNCTIONAL_TESTS = [job.name for job in FUNCTIONAL_TESTS_JOBS]

CORE_BLOCKING_JOB_NAMES = [
    job.name
    for job in FUNCTIONAL_TESTS_JOBS
    if any(
        substr in job.name
        for substr in (
            "_debug, parallel",
            "_binary, parallel",
            "_binary, sequential",
            "_asan_ubsan, db disk, distributed plan, targeted",
        )
    )
] + [
    job.name
    for job in JobConfigs.unittest_jobs
]

# Starts right after its build, like the gate, but is not part of it: its coverage-based
# selection grows with the PR, and on the largest PRs it ran 100-160 minutes, which delayed
# stage 2 by up to an hour. It was the only failing gate job on 2.1% of PR commits, and stage 2
# has integration jobs of its own (the full `arm_binary` run and the other targeted ones).
UNGATED_INTEGRATION_JOB_NAMES = [
    job.name
    for job in JobConfigs.integration_test_targeted_pr_jobs
    if "_asan_ubsan, db disk, targeted" in job.name
]

STYLE_AND_FAST_TESTS = [
    JobNames.STYLE_CHECK,
    JobNames.FAST_TEST,
    *[j.name for j in JobConfigs.tidy_build_arm_jobs],
]

CODE_REVIEW_BLOCKING_JOBS = [
    JobNames.STYLE_CHECK,
    JobNames.FAST_TEST,
]

REGULAR_BUILD_NAMES = [job.name for job in JobConfigs.build_jobs]

PLAIN_FUNCTIONAL_TEST_JOB = [
    j for j in JobConfigs.functional_tests_jobs if "amd_debug, parallel" in j.name
][0]

# Pull requests run the integration tests only in targeted jobs (the changed tests, the
# tests covering the changed lines and the tests that failed in the PR before, each run
# once), except for the full `arm_binary` run below and, with the `ci-coverage` or
# `ci-llvm-coverage` label, the full LLVM coverage run and the tests excluded from it.
INTEGRATION_TARGETED_JOBS = JobConfigs.integration_test_targeted_pr_jobs

# The LLVM coverage jobs run in pull requests only with the `ci-coverage` or `ci-llvm-coverage`
# label (see `should_skip_job`); by default these `arm_binary` jobs run the same configurations instead.
# Master runs the coverage jobs on every commit.
FUNCTIONAL_COVERAGE_REPLACEMENT_JOBS = (
    JobConfigs.functional_tests_arm_binary_coverage_replacement_pr_jobs
)
INTEGRATION_COVERAGE_REPLACEMENT_JOBS = (
    JobConfigs.integration_test_arm_binary_coverage_replacement_pr_jobs
)

PLAIN_INTEGRATION_TEST_JOB = [
    j for j in INTEGRATION_TARGETED_JOBS if "(amd_tsan, targeted)" in j.name
][0]

# The builds the La Casa del Dolor jobs need
DOLOR_BUILD_NAMES = [
    "Build (amd_debug)",
    "Build (arm_asan_ubsan)",
    "Build (amd_tsan)",
    "Build (amd_msan)",
]

workflow = Workflow.Config(
    name="PR",
    event=Workflow.Event.PULL_REQUEST,
    base_branches=[BASE_BRANCH],
    engine=Workflow.Engine.GH_ACTIONS,
    # TODO: revert before merging. Trimmed to the La Casa del Dolor jobs and their builds, to
    # iterate on the BuzzHouse distributed shapes quickly
    jobs=[
        JobConfigs.style_check,
        JobConfigs.code_review.set_run_after([JobNames.STYLE_CHECK]),
        *[job for job in JobConfigs.build_jobs if job.name in DOLOR_BUILD_NAMES],
        *JobConfigs.lacasadeldolor_jobs,
    ],
    artifacts=[
        *ArtifactConfigs.unittests_binaries,
        *ArtifactConfigs.clickhouse_binaries,
        *ArtifactConfigs.clickhouse_darwin_plain_binaries,
        *ArtifactConfigs.clickhouse_debians,
        *ArtifactConfigs.clickhouse_rpms,
        *ArtifactConfigs.clickhouse_tgzs,
        ArtifactConfigs.clickhouse_wasm,
        ArtifactConfigs.wasm_parser,
        ArtifactConfigs.fuzzers,
        ArtifactConfigs.fuzzers_corpus,
        ArtifactConfigs.clickhouse_examples,
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
    enable_merge_ready_status=True,
    enable_gh_summary_comment=True,
    enable_commit_status_on_failure=False,
    enable_open_issues_check=True,
    enable_slack_feed=True,
    pre_hooks=[
        can_be_tested,
        "python3 ./ci/jobs/scripts/workflow_hooks/ci_links.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/store_data.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/review_threads.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/pr_labels_and_category.py",
        configure_performance_shards,
        configure_amd_release_thinlto,
        configure_toolchain_builds,
        "python3 ./ci/jobs/scripts/workflow_hooks/version_log.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/team_notifications.py",
    ],
    workflow_filter_hooks=[should_skip_job],
    post_hooks=[
        "python3 ./ci/jobs/scripts/workflow_hooks/pr_body_check.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/feature_docs.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/new_tests_check.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/can_be_merged.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/can_be_merged.py --review-threads",
        "python3 ./ci/jobs/scripts/workflow_hooks/check_report_messages.py",
    ],
    job_aliases={
        # plain integration test job, no dist plan; runs `--test` as a regular job locally
        "integration": PLAIN_INTEGRATION_TEST_JOB.name,
        "fast": "Fast test",
        "functional": PLAIN_FUNCTIONAL_TEST_JOB.name,
        "build_debug": "Build (amd_debug)",
        "build": "Build (amd_binary)",
    },
    runs_on_label_prefix="pr-",
    ai_orchestrator=Workflow.OrchestratorAI.Config(
        enabled=False,
        provider="bedrock",
        model="global.anthropic.claude-sonnet-5",
    ),
)

# `should_skip_job` skips these builds together with the stress tests that use them, so
# no other job may require their binaries: it would find no artifact to download.
_SINGLE_ARCH_SKIPPED_ARTIFACTS = {
    artifact
    for job in workflow.jobs
    if job.name in PR_SINGLE_ARCH_SKIPPED_BUILDS
    for artifact in job.provides
}
assert len(UNGATED_INTEGRATION_JOB_NAMES) == 1, (
    "the ASan targeted integration job is not found by its name"
)
# TODO: restore with the full job list, the trimmed one has none of these builds
# assert len(_SINGLE_ARCH_SKIPPED_ARTIFACTS) == len(PR_SINGLE_ARCH_SKIPPED_BUILDS), (
#     "every build in PR_SINGLE_ARCH_SKIPPED_BUILDS must be in the PR workflow"
# )
_SINGLE_ARCH_SKIPPED_ARTIFACT_USERS = [
    job.name
    for job in workflow.jobs
    if job.name not in PR_SINGLE_ARCH_SKIPPED_JOBS
    and _SINGLE_ARCH_SKIPPED_ARTIFACTS & set(job.requires or [])
]
assert not _SINGLE_ARCH_SKIPPED_ARTIFACT_USERS, (
    f"jobs {_SINGLE_ARCH_SKIPPED_ARTIFACT_USERS} require binaries of "
    f"{PR_SINGLE_ARCH_SKIPPED_BUILDS}, which pull requests skip: remove the build "
    "from PR_SINGLE_ARCH_SKIPPED_BUILDS in ci/jobs/scripts/workflow_hooks/filter_job.py"
)

WORKFLOWS = [
    workflow,
]
