from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs, ArtifactNames
from ci.defs.job_configs import JobConfigs

# Weekly corpus minimization for the libFuzzer targets.
#
# libFuzzer's `-merge=1` replays every unit of a corpus to find the subset that
# preserves coverage. That is a fixed amount of work proportional to the corpus,
# and for the heavyweight targets it is expensive: on 2026-08-30 it took 38
# minutes for `clickhouse_fuzzer` and 25 minutes for `execute_query_fuzzer`, out
# of the same budget those targets then had left for fuzzing. What it buys is
# small - both corpora shrank by about 1% - so it does not need to happen every
# night, and it must not come out of the nightly fuzzing budget.
#
# Runs on Sunday at 13:13 UTC, well after `NightlyFuzzers` has finished, so that
# the two workflows never read-modify-write the corpora in the artifact bucket
# at the same time.
workflow = Workflow.Config(
    name="WeeklyFuzzersCorpus",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        *[
            j.set_provides([ArtifactNames.ARM_FUZZERS, ArtifactNames.FUZZERS_CORPUS])
            for j in JobConfigs.special_build_jobs
            if "fuzzers" in j.name
        ],
        JobConfigs.libfuzzer_corpus_minimization_job,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    artifacts=[
        ArtifactConfigs.fuzzers,
        ArtifactConfigs.fuzzers_corpus,
        *ArtifactConfigs.clickhouse_binaries,
    ],
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    cron_schedules=["13 13 * * 0"],
)

WORKFLOWS = [
    workflow,
]
