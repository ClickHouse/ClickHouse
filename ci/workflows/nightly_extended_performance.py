"""NightlyExtendedPerformance: nightly `long` performance tests on master, ARM only.

Schedule: 05:13 UTC daily on `arm-medium-storage` (r8g.4xlarge, 500 GB). The pre-hook
`ci/jobs/nightly_extended_performance.py --resolve` picks the binaries: tested = the newest published
`Build (arm_release)` on the first-parent chain from the trigger commit (on a branch, the master commits below its merge
base with master); reference = the tested binary of the last master run of this workflow in the past three nights, so
consecutive runs cover consecutive commits; without one, the newest master build at least 24 h older than the tested one.
No build job.
Tests: `tests/performance` and `tests/benchmarks` as of the tested commit, plus a branch's own changes; harness
scripts from HEAD. The first night after this workflow is merged fails if the tested build predates it (`calibration.xml`).

One job set `master_head`: the `NIGHTLY_PERF_BATCH_JOBS` round-robin batches over the sorted test list, plus one job per extra
name in `NIGHTLY_PERF_PLACEMENT` (`ci/defs/defs.py`, see `tests/performance/README.md`); `calibration.xml` in every job.
After editing either, regenerate the workflow YAML: `PYTHONPATH=./ci:. python3 -m praktika yaml`. The dict is read from HEAD,
the tests from the tested commit: a pinned test absent there is only listed in the job log, an extra job with no existing
test is red, so extra jobs belong on validation branches.
`partial`: failed on the (older) reference only, measured on the tested side only.

Red: any tested-side error (query, setup, missing binary, dataset or reference, test wall clock, required report or
upload), a tested-side timeout, or a non-timeout error in a measured run on either server. Not red: reference-only
errors in settings, setup or prewarm, reference-only timeouts (censored rows).
A failing query is reported as its own run error and skipped; the rest of its test is still measured.
A test whose teardown did not complete, or whose S3 cleanup failed, stops the batch. Changes do not gate and are unconfirmed (no confirmation reruns).

Rerun: `gh workflow run NightlyExtendedPerformance` (`--ref <branch>` runs are excluded from the nightly history).

Results: `query_metrics_v2`, `query_metric_runs_v1`, `perf_test_times_v1`, `perf_metric_changes_v1` with
`workflow_name = 'NightlyExtendedPerformance'`, readable on `play.clickhouse.com` as user `play`; the PR-check
thresholds read `MasterCI` rows only. Query text:
`perf.py --print-queries tests/performance/<test>.xml --queries-to-run <query_index>`. Most regressed last night:

    SELECT test, query_index, query_display_name, old_value, new_value, diff, stat_threshold, new_sha, report_url,
           concat('tests/performance/', test, '.xml') AS test_file
    FROM query_metrics_v2
    WHERE event_date >= today() - 1 AND workflow_name = 'NightlyExtendedPerformance'
      AND report_url LIKE '%?REF=master&%' AND metric = 'client_time' AND diff > stat_threshold
    ORDER BY diff DESC LIMIT 50

Slowest last night:

    SELECT test, query_index, query_display_name, new_value, new_sha, report_url,
           concat('tests/performance/', test, '.xml') AS test_file
    FROM query_metrics_v2
    WHERE event_date >= today() - 1 AND workflow_name = 'NightlyExtendedPerformance'
      AND report_url LIKE '%?REF=master&%' AND metric = 'client_time'
    ORDER BY new_value DESC LIMIT 50

Open: a nightly view on the performance dashboard (it shows `MasterCI` runs only); the size of the runner pool.
"""

from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS
from ci.defs.job_configs import JobConfigs

workflow = Workflow.Config(
    engine=Workflow.Engine.GH_ACTIONS,
    name="NightlyExtendedPerformance",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=JobConfigs.nightly_extended_performance_jobs,
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
    cron_schedules=["13 5 * * *"],
)

WORKFLOWS = [workflow]
