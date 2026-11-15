from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs
from ci.defs.job_configs import JobConfigs

# Publishes the LLVM coverage report of master that https://coverage.clickhouse.com/ serves.
#
# The jobs are the shared coverage jobs from `JobConfigs`, unchanged: the coverage
# build, the unit, stateless and integration coverage shards, and the final
# `LLVM Coverage` job that merges all shard profiles, generates the HTML report and
# inserts the master row (`pull_request_number = 0`, `branch = 'master'`) into
# `coverage_ci.coverage_data`. The row's `coverage_report_url` and the S3 location are
# derived from the workflow name by praktika (`REFs/master/<sha>/<workflow>/llvm_coverage/...`),
# so the site, which reads the URL from the row, needs no change.
#
# The coverage build is not forced here. `enable_cache` makes praktika look up a
# successful record by job name and digest, and the digest of the build covers only
# the compiled sources (`build_digest_config`), so a run on a commit that changes no
# compiled source reuses the newest existing build (and the shard profiles whose
# inputs did not change) instead of building again. A run on a commit that changes
# compiled sources must rebuild: the report maps profile counters to source lines
# and is only valid for the exact sources the binary was built from.

# The S3 + DBReplicated, ParallelReplicas and AsyncInsert coverage shards are
# parametrized into `functional_tests_jobs` next to the sanitizer jobs.
FUNCTIONAL_TESTS_LLVM_COVERAGE_S3_JOBS = [
    job for job in JobConfigs.functional_tests_jobs if "amd_llvm_coverage" in job.name
]

workflow = Workflow.Config(
    name="CoverageReport",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    engine=Workflow.Engine.GH_ACTIONS,
    jobs=[
        *JobConfigs.build_llvm_coverage_job,
        *JobConfigs.unittest_llvm_coverage_job,
        *JobConfigs.functional_test_llvm_coverage_jobs,
        *FUNCTIONAL_TESTS_LLVM_COVERAGE_S3_JOBS,
        *JobConfigs.integration_test_llvm_coverage_jobs,
        JobConfigs.llvm_coverage_job,
    ],
    artifacts=[
        *ArtifactConfigs.unittests_binaries,
        *ArtifactConfigs.clickhouse_binaries,
        *ArtifactConfigs.llvm_profdata_file,
        ArtifactConfigs.llvm_coverage_info_file,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    enable_slack_feed=True,
    cron_schedules=["0 */3 * * *"],
)

WORKFLOWS = [
    workflow,
]
