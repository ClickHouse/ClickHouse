from praktika import Job, Workflow

from ci.defs.defs import (
    BASE_BRANCH,
    DOCKERS,
    SECRETS,
    ArtifactConfigs,
    ArtifactNames,
    BuildTypes,
    RunnerLabels,
)
from ci.defs.job_configs import JobConfigs

# The order is traced on the release build: it is linked with ThinLTO, like the binaries we ship, and
# ThinLTO renames the local functions that it promotes (the `.llvm.<hash>` suffix), so the names of
# another build do not match them.
release_build_job = Job.Config.get_job(
    JobConfigs.release_build_jobs, f"Build ({BuildTypes.ARM_RELEASE})"
).set_provides(ArtifactNames.CH_ARM_RELEASE, reset=True)

# Runs directly on the runner (no container), like the other `enable_gh_auth` jobs: the tracer needs
# only python3 and `ptrace` of its own child process, and the pull-request management needs `gh`,
# which the runner provides. `enable_gh_auth` supplies the GitHub App token used to push the branch
# and open the pull request. Requiring the artifact orders it after the build.
startup_symbol_order_job = Job.Config(
    name="Startup symbol order",
    runs_on=RunnerLabels.FUNC_TESTER_ARM,
    command="python3 ./ci/jobs/startup_symbol_order_nightly.py",
    requires=[ArtifactNames.CH_ARM_RELEASE],
    enable_gh_auth=True,
)

workflow = Workflow.Config(
    name="NightlyStartupSymbolOrder",
    engine=Workflow.Engine.GH_ACTIONS,
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        release_build_job,
        startup_symbol_order_job,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    artifacts=[
        *ArtifactConfigs.clickhouse_binaries,
    ],
    enable_report=True,
    enable_cidb=False,
    cron_schedules=["43 5 * * *"],  # off-peak, distinct from the other nightlies
)

WORKFLOWS = [
    workflow,
]
