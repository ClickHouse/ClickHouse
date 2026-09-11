from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs
from ci.defs.job_configs import JobConfigs

# Weekly Control-Flow Integrity (CFI) check.
# Builds the monolithic ClickHouse binary with Clang CFI (cfi-vcall, cfi-derived-cast)
# using the release compile profile (ThinLTO + -fwhole-program-vtables) and runs
# stateless, integration, stress, AST fuzzer and BuzzHouse tests against it. A CFI
# violation traps (SIGILL) and aborts the server, which surfaces as a job failure;
# -DSPLIT_DEBUG_SYMBOLS=ON keeps the core-dump stack trace pointing at the offending
# virtual call or bad cast.
#
# Scope: this covers the single self-extracting `clickhouse` binary only. It is not a
# full release-artifact build -- it omits -DBUILD_STANDALONE_KEEPER=1, so the standalone
# `clickhouse-keeper` executable is not built; integration tests exercise Keeper via
# `clickhouse keeper` mode (the same coordination code linked into the monolith), not
# the standalone binary.
#
# This is the only test job that runs against a ThinLTO binary, which is what official
# release builds use, so besides CFI violations it also catches ThinLTO-only behavior
# differences (e.g. the `__functional` frame-name suppression found by `test_crash_log`,
# fixed in #112152) that no other CI job can see. Stateless tests whose exact-output
# assertions cannot hold in this build profile carry the no-cfi tag (clickhouse-test
# detects the CFI build from `system.build_options` and skips them).
#
# Runs every Monday at 03:00 UTC.
workflow = Workflow.Config(
    name="WeeklyCFI",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        *JobConfigs.cfi_build_job,
        *JobConfigs.cfi_stateless_jobs,
        *JobConfigs.cfi_integration_jobs,
        *JobConfigs.cfi_stress_job,
        *JobConfigs.cfi_ast_fuzzer_job,
        *JobConfigs.cfi_buzz_fuzzer_job,
    ],
    artifacts=[
        *ArtifactConfigs.clickhouse_binaries,
        *ArtifactConfigs.clickhouse_debians,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    cron_schedules=["0 3 * * 1"],
    pre_hooks=["python3 ./ci/jobs/scripts/workflow_hooks/store_data.py"],
)

WORKFLOWS = [
    workflow,
]
