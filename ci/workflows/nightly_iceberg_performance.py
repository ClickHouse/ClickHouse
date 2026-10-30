"""NightlyIcebergPerformance: the performance tests with their MergeTree tables created as Iceberg tables, ARM only.

The Iceberg configuration (`perf.py --table-engine`, see the "Iceberg configuration" section of
`tests/performance/README.md`) in two backends: `iceberg_local` (`IcebergLocal`) and `iceberg_s3` (`IcebergS3` on the
job-local S3 endpoint). Tests: those with a plain MergeTree table in a `create_query`, minus `long` ones and
`tests/performance/iceberg_skip_list.txt`, round-robin over the jobs of each backend; every query of every test.

Binaries, tests and reference as in `NightlyExtendedPerformance` (the same pre-hook): reference = the tested binary of
the last master run of this workflow in the past three nights. Changes do not gate.

Results: the tables of `NightlyExtendedPerformance` with `workflow_name = 'NightlyIcebergPerformance'` and
`arch = 'arm_iceberg_local'` or `'arm_iceberg_s3'`.

Rerun: `gh workflow run NightlyIcebergPerformance`.
"""

from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS
from ci.defs.job_configs import JobConfigs

workflow = Workflow.Config(
    engine=Workflow.Engine.GH_ACTIONS,
    name="NightlyIcebergPerformance",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=JobConfigs.nightly_iceberg_performance_jobs,
    dockers=DOCKERS,
    disable_dockers_build=True,
    enable_cache=False,
    secrets=SECRETS,
    enable_report=True,
    enable_cidb=True,
    pre_hooks=[
        "python3 ./ci/jobs/scripts/workflow_hooks/store_data.py",
        "python3 ./ci/jobs/nightly_extended_performance.py --resolve",
    ],
    cron_schedules=["13 1 * * *"],
)

WORKFLOWS = [workflow]
