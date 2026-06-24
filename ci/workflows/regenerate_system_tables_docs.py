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

# Only the plain ARM binary build is needed: the job introspects system tables
# from the binary, it does not run tests. Select it by what it provides rather
# than by name so it keeps working if the build job is renamed.
arm_binary_build = [
    j for j in JobConfigs.build_jobs if ArtifactNames.CH_ARM_BINARY in j.provides
]

regenerate_job = Job.Config(
    name="Regenerate system tables docs",
    # Bare style-check runner (no docker): it has `gh` + python3 and can run the
    # ARM binary. This mirrors the other PR-opening maintenance jobs (Hourly,
    # NightlyUpload), which run here with enable_gh_auth. The docs-builder image
    # used by docs_job.py does NOT ship `gh`, so it is deliberately not used.
    runs_on=RunnerLabels.STYLE_CHECK_ARM,
    command="python3 ./ci/jobs/regenerate_system_tables_docs.py",
    requires=[ArtifactNames.CH_ARM_BINARY],
    enable_gh_auth=True,
)

workflow = Workflow.Config(
    name="RegenerateSystemTablesDocs",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        *arm_binary_build,
        regenerate_job,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    artifacts=[
        *ArtifactConfigs.clickhouse_binaries,
    ],
    enable_cache=True,
    enable_report=True,
    enable_cidb=False,
    # Mondays 07:00 UTC. Weekly + low-noise to start; raise frequency if needed.
    cron_schedules=["0 7 * * 1"],
)

WORKFLOWS = [
    workflow,
]
