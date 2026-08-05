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
            j.set_provides([ArtifactNames.ARM_FUZZERS, ArtifactNames.FUZZERS_CORPUS])
            for j in JobConfigs.special_build_jobs
            if "fuzzers" in j.name
        ],
        # The libFuzzer test job generates the fuzzer dictionary from the release
        # binary, so this workflow needs to provide it. The build is normally a
        # cache hit against the same commit built in MasterCI (its config is left
        # unchanged so the digest matches).
        *[j for j in JobConfigs.release_build_jobs if "arm_release" in j.name],
        JobConfigs.libfuzzer_job,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    artifacts=[
        ArtifactConfigs.fuzzers,
        ArtifactConfigs.fuzzers_corpus,
        *ArtifactConfigs.clickhouse_binaries,
        *ArtifactConfigs.clickhouse_debians,
        *ArtifactConfigs.clickhouse_rpms,
        *ArtifactConfigs.clickhouse_tgzs,
    ],
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    cron_schedules=["13 3 * * *"],
)

WORKFLOWS = [
    workflow,
]
