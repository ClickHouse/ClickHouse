from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs, ArtifactNames
from ci.defs.job_configs import JobConfigs

# TODO: add alert on workflow failure

workflow = Workflow.Config(
    name="NightlyFuzzers",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        JobConfigs.fuzzers_build_job.set_provides([ArtifactNames.FUZZERS, ArtifactNames.FUZZERS_CORPUS]),
        JobConfigs.libfuzzer_job,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    artifacts=[
        ArtifactConfigs.fuzzers,
        ArtifactConfigs.fuzzers_corpus,
    ],
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    cron_schedules=["13 3 * * *"],
)

WORKFLOWS = [
    workflow,
]
