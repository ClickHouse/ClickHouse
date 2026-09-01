from praktika import Workflow

from ci.defs.defs import (
    DOCKERS,
    SECRETS,
    ArtifactConfigs,
    with_long_retention_tags,
)
from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.workflow_hooks.filter_job import should_skip_job

builds_for_release_branch = [
    job
    for job in JobConfigs.build_jobs
    if "coverage" not in job.name and "binary" not in job.name
] + JobConfigs.release_build_jobs

clickhouse_binaries_with_tags = with_long_retention_tags(
    ArtifactConfigs.clickhouse_binaries
)

workflow = Workflow.Config(
    name="ReleaseBranchCI",
    event=Workflow.Event.PUSH,
    branches=["2[1-9].[1-9][0-9]", "2[1-9].[1-9]"],
    engine=Workflow.Engine.GH_ACTIONS,
    jobs=[
        *builds_for_release_branch,
        *[
            job
            for job in JobConfigs.special_build_jobs
            if any(t in job.name for t in ("darwin",))
        ],
        *JobConfigs.sign_macos_binary_jobs,
        JobConfigs.docker_server,
        JobConfigs.docker_keeper,
        *JobConfigs.install_check_master_jobs,
        *[job for job in JobConfigs.functional_tests_jobs if "asan" in job.name],
        *[job for job in JobConfigs.unittest_jobs if "fuzzer" not in job.name],
        *[
            job
            for job in JobConfigs.integration_test_asan_master_jobs
            if "asan" in job.name
        ],
        *[
            job
            for job in JobConfigs.integration_test_jobs_required
            if any(t in job.name for t in ("asan", "release"))
        ],
        *[
            job
            for job in JobConfigs.integration_test_jobs_non_required
            if "tsan" in job.name
        ],
        *JobConfigs.stress_test_jobs,
    ],
    artifacts=[
        *ArtifactConfigs.unittests_binaries,
        *clickhouse_binaries_with_tags,
        *ArtifactConfigs.clickhouse_darwin_plain_binaries,
        *ArtifactConfigs.clickhouse_darwin_signed_zips,
        *ArtifactConfigs.clickhouse_debians,
        *ArtifactConfigs.clickhouse_rpms,
        *ArtifactConfigs.clickhouse_tgzs,
    ],
    dockers=DOCKERS,
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
        "python3 ./ci/jobs/scripts/workflow_hooks/set_parent_pr_number.py",
    ],
    workflow_filter_hooks=[should_skip_job],
    post_hooks=[],
)

WORKFLOWS = [
    workflow,
]
