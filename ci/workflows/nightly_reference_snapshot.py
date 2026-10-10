"""Build, export, and publish the Head reference-docs snapshot each night."""

from praktika import Workflow

from ci.defs.defs import (
    BASE_BRANCH,
    DOCKERS,
    SECRETS,
    ArtifactConfigs,
    ArtifactNames,
)
from ci.defs.job_configs import JobConfigs


# Export from the same ARM binary artifact that a pull request validates. This
# makes Head publication a trusted transport of the exact system-table output,
# rather than another docs build with a different binary.
arm_binary_build = next(
    job for job in JobConfigs.build_jobs if ArtifactNames.CH_ARM_BINARY in job.provides
)

workflow = Workflow.Config(
    name="NightlyReferenceSnapshot",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    engine=Workflow.Engine.GH_ACTIONS,
    jobs=[
        arm_binary_build,
        JobConfigs.reference_snapshot_job,
        JobConfigs.publish_reference_snapshot_job,
    ],
    artifacts=[
        *ArtifactConfigs.clickhouse_binaries,
        ArtifactConfigs.reference_snapshot,
    ],
    dockers=DOCKERS,
    # Praktika emits workflow_dispatch for scheduled workflows, so this is also
    # manually runnable from the Actions UI. The writer credential is attached
    # only to publish_reference_snapshot_job above.
    secrets=SECRETS,
    enable_report=True,
    enable_cidb=False,
    cron_schedules=["23 5 * * *"],
)

WORKFLOWS = [workflow]
