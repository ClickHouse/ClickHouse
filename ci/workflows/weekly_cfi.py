from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs
from ci.defs.job_configs import JobConfigs

# Weekly Control-Flow Integrity (CFI) check.
# Builds the monolithic ClickHouse binary with Clang CFI (cfi-vcall, cfi-derived-cast)
# using the release compile profile (ThinLTO + -fwhole-program-vtables) and runs
# integration and stress tests against it. A CFI violation traps (SIGILL) and aborts the
# server, which surfaces as a job failure; -DSPLIT_DEBUG_SYMBOLS=ON keeps the core-dump
# stack trace pointing at the offending virtual call or bad cast.
#
# Scope: this covers the single self-extracting `clickhouse` binary only. It is not a
# full release-artifact build -- it omits -DBUILD_STANDALONE_KEEPER=1, so the standalone
# `clickhouse-keeper` executable is not built; integration tests exercise Keeper via
# `clickhouse keeper` mode (the same coordination code linked into the monolith), not
# the standalone binary.
#
# The stateless functional suite is intentionally excluded: its exact-output assertions
# produce build-profile false positives (e.g. query-profiler symbolization) and pick up
# master-flaky tests, none of which are CFI violations, so they would keep the job red
# for non-CFI reasons. Adding stateless later would need a curated no-cfi skip set, the
# way asan/tsan/msan carry per-build skips.
#
# Runs every Monday at 03:00 UTC.
workflow = Workflow.Config(
    name="WeeklyCFI",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        *JobConfigs.cfi_build_job,
        *JobConfigs.cfi_integration_jobs,
        *JobConfigs.cfi_stress_job,
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
