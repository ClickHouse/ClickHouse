from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs, ArtifactNames
from ci.defs.job_configs import JobConfigs

# TODO: add alert on workflow failure

workflow = Workflow.Config(
    name="NightlyFuzzers",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        *[
            j.set_provides([ArtifactNames.FUZZERS, ArtifactNames.FUZZERS_CORPUS])
            for j in JobConfigs.special_build_jobs
            if "fuzzers" in j.name
        ],
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
