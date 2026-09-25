"""NightlyExtendedPerformance: nightly `long` performance tests and ClickBench on master, ARM only.

Schedule: 05:13 UTC daily on `arm-large-storage` (m8g.8xlarge, 500 GB). The pre-hook
`ci/jobs/nightly_extended_performance.py --resolve` picks the tested binary: the newest published
`Build (arm_release)` on the first-parent chain from the trigger commit (on a branch, the master commits below it).
No build job.

Job sets, 4 batches each over the sorted test list, `calibration.xml` in every batch:
- `master_head` (alert reference): reference = the previous night's tested binary.
- `release_base` (informational, no `query_metrics_v2` rows): reference = the build of the release branch base
  (`release_branch_base_sha_with_predecessors`, as in `MasterCI`).
- `ClickBench (arm_release)`: praktika `Result` rows keyed by the trigger commit, the tested SHA in `info`.
`partial`: failed on the (older) reference only, measured on the tested side only.

Red: any tested-side error (query, setup, missing binary, dataset or reference, test wall clock, required report or
upload), a tested-only timeout, or a non-timeout error in a measured run on either server. Not red: reference-only
errors in settings, setup or prewarm, reference-only timeouts, double timeouts (censored rows).
A test whose teardown did not complete stops the batch. Changes do not gate and are unconfirmed (no confirmation reruns).

Rerun: `gh workflow run NightlyExtendedPerformance` (`--ref <branch>` runs are excluded from the nightly history).

Results: `query_metrics_v2`, `query_metric_runs_v1`, `perf_test_times_v1`, `perf_metric_changes_v1` with
`workflow_name = 'NightlyExtendedPerformance'`; the PR-check thresholds read `MasterCI` rows only. Query text:
`perf.py --print-queries tests/performance/<test>.xml --queries-to-run <query_index>`. Most regressed last night:

    SELECT test, query_index, query_display_name, old_value, new_value, diff, stat_threshold, new_sha, report_url,
           concat('tests/performance/', test, '.xml') AS test_file
    FROM query_metrics_v2
    WHERE event_date >= today() - 1 AND workflow_name = 'NightlyExtendedPerformance'
      AND metric = 'client_time' AND diff > stat_threshold
    ORDER BY diff DESC LIMIT 50

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
    jobs=[*JobConfigs.nightly_extended_performance_jobs, JobConfigs.nightly_clickbench_arm_job],
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
