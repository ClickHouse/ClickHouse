from praktika import Workflow

from ci.defs.defs import DOCKERS, SECRETS, ArtifactConfigs
from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.workflow_hooks.filter_job import should_skip_job

workflow = Workflow.Config(
    name="BackportPR",
    event=Workflow.Event.PULL_REQUEST,
    base_branches=["2[1-9].[1-9][0-9]", "2[1-9].[1-9]"],
    jobs=[
        *[
            job
            for job in JobConfigs.build_jobs
            if any(t in job.name for t in ("amd_asan", "amd_tsan", "release", "debug"))
        ],
        *[
            job
            for job in JobConfigs.special_build_jobs
            if any(t in job.name for t in ("darwin",))
        ],
        JobConfigs.docker_sever,
        JobConfigs.docker_keeper,
        *JobConfigs.install_check_jobs,
        *JobConfigs.compatibility_test_jobs,
        *[job for job in JobConfigs.functional_tests_jobs if "asan" in job.name],
        *[job for job in JobConfigs.stress_test_jobs if "tsan" in job.name],
        *[
            job
            for job in JobConfigs.integration_test_jobs_required
            if "asan" in job.name
        ],
        *[
            job
            for job in JobConfigs.integration_test_jobs_non_required
            if "tsan" in job.name
        ],
    ],
    artifacts=[
        *ArtifactConfigs.unittests_binaries,
        *ArtifactConfigs.clickhouse_binaries,
        *ArtifactConfigs.clickhouse_debians,
        *ArtifactConfigs.clickhouse_rpms,
        *ArtifactConfigs.clickhouse_tgzs,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_job_filtering_by_changes=True,
    enable_cache=True,
    enable_report=True,
    enable_automerge=True,
    enable_cidb=True,
    enable_commit_status_on_failure=True,
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
