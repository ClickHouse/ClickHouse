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
    ],
    artifacts=[
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
        "python3 ./ci/jobs/scripts/workflow_hooks/set_dummy_sync_commit_status.py",
    ],
)

WORKFLOWS = [
    workflow,
]
