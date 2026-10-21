from praktika import Job, Workflow

from ci.defs.defs import (
    BASE_BRANCH,
    DOCKERS,
    GH_AUTH_TRUSTED_LAMBDA_NAME,
    SECRETS,
    ArtifactConfigs,
    ArtifactNames,
    RunnerLabels,
)
from ci.defs.job_configs import JobConfigs

# General nightly workflow. Hosts independent housekeeping and documentation jobs
# that run once a day against master:
#   - Prepare changelog: prepares CHANGELOG.md for the upcoming release
#     (see ci/jobs/changelog_nightly.py).
#   - Label external contributors: labels issues and pull requests from authors
#     outside the organization (see ci/jobs/label_external_contributors.py).
#   - Docs reference snapshot: builds the ARM binary, exports its reference
#     system-table snapshot, and publishes Head to Vercel Blob.
# Add further nightly jobs here rather than creating a new workflow each time.

# Export from the same ARM binary artifact that a pull request validates. This
# makes Head publication a trusted transport of the exact system-table output,
# rather than another docs build with a different binary.
arm_binary_build = next(
    job for job in JobConfigs.build_jobs if ArtifactNames.CH_ARM_BINARY in job.provides
)

workflow = Workflow.Config(
    name="Nightly",
    engine=Workflow.Engine.GH_ACTIONS,
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    gh_auth_lambda_name=GH_AUTH_TRUSTED_LAMBDA_NAME,
    jobs=[
        # Publishing docs is best-effort. It must be visible when it fails, but
        # must not turn the shared Nightly workflow red or hold up its
        # housekeeping jobs. PR snapshot validation remains required.
        arm_binary_build.set_allow_failure().set_continue_on_error(),
        JobConfigs.reference_snapshot_job.set_allow_failure().set_continue_on_error(),
        JobConfigs.publish_reference_snapshot_job.set_allow_failure().set_continue_on_error(),
        Job.Config(
            name="Prepare changelog",
            command="python3 ./ci/jobs/changelog_nightly.py",
            runs_on=RunnerLabels.ARM_TINY,
            enable_gh_auth=True,
        ),
        Job.Config(
            name="Label external contributors",
            command="python3 ./ci/jobs/label_external_contributors.py",
            runs_on=RunnerLabels.ARM_TINY,
            enable_gh_auth=True,
        ),
    ],
    artifacts=[
        *ArtifactConfigs.clickhouse_binaries,
        ArtifactConfigs.reference_snapshot,
    ],
    dockers=DOCKERS,
    # The docs pipeline can use the already-published builder images. Avoid
    # injecting Docker build jobs into this shared Nightly workflow: they are
    # prerequisites only for the best-effort docs path, not for maintenance.
    disable_dockers_build=True,
    secrets=SECRETS,
    enable_report=True,
    enable_cidb=False,
    cron_schedules=["23 2 * * *"],
)

WORKFLOWS = [
    workflow,
]
