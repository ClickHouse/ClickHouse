from praktika import Workflow

from ci.defs.defs import (
    BASE_BRANCH,
    DOCKERS,
    SECRETS,
    ArtifactConfigs,
    ArtifactNames,
    with_long_retention_tags,
)
from ci.defs.job_configs import JobConfigs

# TODO: add alert on workflow failure

workflow = Workflow.Config(
    name="NightlyFuzzers",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    engine=Workflow.Engine.GH_ACTIONS,
    jobs=[
        *[
            j.set_provides([ArtifactNames.ARM_FUZZERS, ArtifactNames.FUZZERS_CORPUS])
            for j in JobConfigs.special_build_jobs
            if "fuzzers" in j.name
        ],
        # The libFuzzer test job generates the fuzzer dictionary from the release
        # binary, so this workflow needs to provide it. Take the same job variant
        # MasterCI takes, so the digest matches and the same commit's build is a
        # cache hit here instead of being built a second time.
        *[
            j
            for j in JobConfigs.release_build_jobs_with_examples
            if "arm_release" in j.name
        ],
        JobConfigs.libfuzzer_job,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    artifacts=[
        ArtifactConfigs.fuzzers,
        ArtifactConfigs.fuzzers_corpus,
        *with_long_retention_tags(ArtifactConfigs.clickhouse_binaries),
        *ArtifactConfigs.clickhouse_debians,
        *ArtifactConfigs.clickhouse_rpms,
        *ArtifactConfigs.clickhouse_tgzs,
        ArtifactConfigs.clickhouse_examples,
    ],
    # Mangle appends the docker job names to every job's run_after, and run_after
    # is part of the job digest, so a workflow whose docker graph differs from
    # MasterCI's cannot hit its cache entry for the shared release build.
    enable_dockers_manifest_merge=True,
    # The merge job's own digest does not cover this flag, so the two workflows
    # would share a cache entry while tagging `latest` differently.
    set_latest_for_docker_merged_manifest=True,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    cron_schedules=["13 3 * * *"],
)

WORKFLOWS = [
    workflow,
]
