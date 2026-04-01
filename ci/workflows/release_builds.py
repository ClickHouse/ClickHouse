from praktika import Workflow

from ci.defs.defs import DOCKERS, SECRETS, ArtifactConfigs
from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.workflow_hooks.filter_job import should_skip_job

builds_for_release_branch = [
    job.unset_provides("unittest")
    for job in JobConfigs.build_jobs + JobConfigs.release_build_jobs
    if "coverage" not in job.name
]

# Make sure that builds that get tested are built first
# Note that the release build job should not block or be blocked, it is long and it's dependencies are fast.
BLOCKING_BUILD_JOBS = [
    job.name
    for job in JobConfigs.build_jobs
    if any(substr in job.name for substr in ["binary"])
]

workflow = Workflow.Config(
    name="Release Builds",
    event=Workflow.Event.DISPATCH,
    jobs=[
        *[
            job.set_dependency(
                BLOCKING_BUILD_JOBS
                if job.name not in BLOCKING_BUILD_JOBS and "release" not in job.name
                else []
            )
            for job in builds_for_release_branch
        ],
        JobConfigs.docker_server,
        JobConfigs.docker_keeper,
        *JobConfigs.install_check_master_jobs,
        *[
            job
            for job in JobConfigs.functional_tests_jobs
            if any(t in job.name for t in ("release", "binary"))
        ],
    ],
    additional_jobs=["GrypeScan", "SignRelease", "CIReport", "SourceUpload"],
    artifacts=[
        *ArtifactConfigs.clickhouse_binaries,
        *ArtifactConfigs.clickhouse_stripped_binaries,
        *ArtifactConfigs.clickhouse_debians,
        *ArtifactConfigs.clickhouse_rpms,
        *ArtifactConfigs.clickhouse_tgzs,
    ],
    dockers=DOCKERS,
    enable_dockers_manifest_merge=True,
    secrets=SECRETS,
    enable_job_filtering_by_changes=False,
    enable_cache=False,
    enable_report=True,
    enable_cidb=True,
    enable_merge_ready_status=False,  # NOTE (strtgbb): we don't use this, TODO, see if we can use it
    enable_gh_summary_comment=False,
    enable_commit_status_on_failure=True,
    enable_open_issues_check=False,
    enable_slack_feed=False,
    pre_hooks=[
        "python3 ./ci/jobs/scripts/workflow_hooks/store_data.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/version_log.py",
    ],
    workflow_filter_hooks=[should_skip_job],
    post_hooks=[],
)

WORKFLOWS = [
    workflow,
]
