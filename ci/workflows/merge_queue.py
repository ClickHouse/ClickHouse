from praktika import Workflow

from ci.defs.defs import DOCKERS, SECRETS, ArtifactConfigs, ArtifactNames, JobNames
from ci.defs.job_configs import JobConfigs

workflow = Workflow.Config(
    name="MergeQueueCI",
    event=Workflow.Event.MERGE_QUEUE,
    jobs=[
        JobConfigs.style_check,
        JobConfigs.fast_test,
        *[job for job in JobConfigs.build_jobs if job.name == "Build (amd_binary)"],
    ],
    artifacts=[
        *[
            a
            for a in ArtifactConfigs.clickhouse_binaries
            if a.name == ArtifactNames.CH_AMD_BINARY
        ],
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    enable_merge_ready_status=True,
    enable_commit_status_on_failure=True,
    pre_hooks=[
        "python3 ./ci/jobs/scripts/workflow_hooks/store_data.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/set_dummy_sync_commit_status.py",
    ],
)

WORKFLOWS = [
    workflow,
]
