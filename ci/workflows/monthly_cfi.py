from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs, ArtifactNames
from ci.defs.job_configs import JobConfigs

# Monthly Control-Flow Integrity (CFI) check.
# Builds ClickHouse with Clang CFI (cfi-vcall, cfi-derived-cast) on top of a release
# build (ThinLTO + -fwhole-program-vtables already enabled) and runs the full stateless
# functional test suite. A CFI violation aborts the server with a diagnostic message,
# which surfaces as a test failure here.
#
# This workflow is intentionally lightweight: one build + one test run, monthly cadence.
# Only merge this workflow if CFI is confirmed to not produce false positives.

workflow = Workflow.Config(
    name="MonthlyCFI",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        *JobConfigs.cfi_build_job,
        *JobConfigs.cfi_stateless_jobs,
    ],
    artifacts=[
        *ArtifactConfigs.clickhouse_binaries,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    cron_schedules=["0 6 1 * *"],  # 06:00 UTC on the 1st of each month
)

WORKFLOWS = [
    workflow,
]
