from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs
from ci.defs.job_configs import JobConfigs

# Weekly Control-Flow Integrity (CFI) check.
# Builds the monolithic ClickHouse binary with all Clang CFI checks (`ENABLE_CFI`, glibc with
# `GLIBC_COMPATIBILITY`) using the release compile profile (ThinLTO + -fwhole-program-vtables) and
# runs integration and stress tests against it. The build uses `ENABLE_CFI_DIAGNOSTICS`: instead of
# trapping, a CFI violation is reported through the UBSan runtime (`runtime error: control flow
# integrity check ...`, then `SUMMARY: UndefinedBehaviorSanitizer`) and the server continues, so one
# run finds every violation, with its source location and the type that was expected. XRay is
# disabled because it is incompatible with the diagnostics runtime, as it is in the sanitizer builds.
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
    engine=Workflow.Engine.GH_ACTIONS,
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
