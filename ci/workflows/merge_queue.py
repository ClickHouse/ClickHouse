from praktika import Workflow

from ci.defs.defs import SECRETS, ArtifactConfigs, ArtifactNames, JobNames
from ci.defs.job_configs import JobConfigs

workflow = Workflow.Config(
    name="MergeQueueCI",
    event=Workflow.Event.MERGE_QUEUE,
    jobs=[
        JobConfigs.docker_build_arm,
        JobConfigs.docker_build_amd,
        JobConfigs.style_check,
        JobConfigs.fast_test,
        *[job for job in JobConfigs.build_jobs if job.name == "Build (amd_binary)"],
        *[
            job
            for job in JobConfigs.unittest_jobs
            if job.name == JobNames.UNITTEST + " (binary)"
        ],
    ],
    artifacts=[
        *[
            a
            for a in ArtifactConfigs.unittests_binaries
            if a.name == ArtifactNames.UNITTEST_AMD_BINARY
        ],
        *[
            a
            for a in ArtifactConfigs.clickhouse_binaries
            if a.name == ArtifactNames.CH_AMD_BINARY
        ],
        ArtifactConfigs.fast_test,
    ],
    # dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    enable_merge_ready_status=True,
    enable_commit_status_on_failure=True,
    pre_hooks=[
        "python3 ./ci/jobs/scripts/workflow_hooks/store_data.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/set_dummy_ch_inc_commit_status.py",
    ],
)

WORKFLOWS = [
    workflow,
]
