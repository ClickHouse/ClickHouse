from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs
from ci.defs.job_configs import JobConfigs

workflow = Workflow.Config(
    name="OptimizeToolchain",
    event=Workflow.Event.DISPATCH,
    branches=[BASE_BRANCH],
    jobs=[
        *JobConfigs.toolchain_build_jobs,
        JobConfigs.update_toolchain_dockerfile_job.set_dependency(
            [j.name for j in JobConfigs.toolchain_build_jobs]
        ),
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    artifacts=[
        ArtifactConfigs.toolchain_pgo_bolt_amd,
        ArtifactConfigs.toolchain_pgo_bolt_arm,
    ],
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
)

WORKFLOWS = [
    workflow,
]
