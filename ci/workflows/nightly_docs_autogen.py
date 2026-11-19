from praktika import Job, Workflow

from ci.defs.defs import (
    BASE_BRANCH,
    DOCKERS,
    SECRETS,
    ArtifactConfigs,
    ArtifactNames,
    RunnerLabels,
)
from ci.defs.job_configs import JobConfigs

# Reuse the regular ARM binary build so the generator queries `system.documentation`
# from a fresh master binary.
arm_binary_build = next(
    job for job in JobConfigs.build_jobs if ArtifactNames.CH_ARM_BINARY in job.provides
)

# Runs directly on the runner (no container), matching the other `enable_gh_auth`
# jobs: the generator only needs python3 and the built `clickhouse` binary, and
# the pull-request management needs `gh`, which the runner provides. `enable_gh_auth`
# supplies the GitHub App token used to push the branch and open the pull request.
# Requiring the artifact orders it after the build.
docs_autogen_job = Job.Config(
    name="Docs autogenerate",
    runs_on=RunnerLabels.FUNC_TESTER_ARM,
    command="python3 ./ci/jobs/docs_autogen_nightly.py",
    requires=[ArtifactNames.CH_ARM_BINARY],
    enable_gh_auth=True,
)

workflow = Workflow.Config(
    name="NightlyDocsAutogenerate",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        arm_binary_build,
        docs_autogen_job,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    artifacts=[
        *ArtifactConfigs.clickhouse_binaries,
    ],
    enable_report=True,
    enable_cidb=False,
    cron_schedules=["13 7 * * *"],  # off-peak, distinct from the other nightlies
)

WORKFLOWS = [
    workflow,
]
