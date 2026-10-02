# Working with CI (praktika) from an agent

Notes for reading CI results and for writing job scripts. Everything here is
reachable from a terminal - no browser needed.

## Reading a run's results

The HTML report (`json.html?REF=…&sha=…&name_0=<workflow>&name_1=<job>`) is only a
renderer for one public JSON file. Fetch that instead:

```bash
curl -s "https://s3.amazonaws.com/clickhouse-test-reports/REFs/<ref>/<sha>/result_<normalized_workflow_name>.json"
```

`<normalized_workflow_name>` is `Utils.normalize_string(workflow.name)` (lowercase,
non-alphanumerics to `_`), e.g. `result_nightlysqlancer.json`. In that file each
job is an entry under `results`, with its `status`, `info`, sub-results and
`links`.

- **Always follow `links`; never build an artifact URL by hand.** Files attached
  to a *sub-result* are uploaded under an extra `normalize_string(<row name>)`
  prefix (`_ResultS3.upload_result_files_to_s3`), so guessing the job directory
  returns S3 `AccessDenied` - which looks like a permissions problem but is just
  a missing key.
- Only **failed leaves** survive in the workflow report: OK sub-results are
  dropped and a row that has children is replaced by its children
  (`Result._flat_failed_leaves`). If a grouping row must stay visible, emit it as
  a leaf with no children.
- Raw job logs: `gh api repos/ClickHouse/ClickHouse/actions/jobs/<job_id>/logs`.
  `gh run view --job <id> --log` returns nothing for long jobs, and a *running*
  job exposes no logs at all - wait for it to finish.
- History across runs is in CI DB, readable without credentials:

```bash
curl -s 'https://play.clickhouse.com/?user=play&default_format=TSV' --data-binary \
  "SELECT check_start_time, test_name, test_status FROM default.checks
   WHERE check_name = 'SQLancer (arm_release)' AND check_start_time > now() - INTERVAL 30 DAY
   ORDER BY check_start_time DESC LIMIT 20"
```

  Every job produces one row per report row (`CIDB.json_data_generator`), with
  `test_name` = the row name. Stable row names are therefore what makes
  "when did this first appear" answerable - avoid embedding counts or timestamps
  in them.

## Triggering a workflow on a branch

`gh workflow run <WorkflowName> --ref <branch>` works for any branch:
`Workflow.Config(branches=...)` only gates the push trigger. The run uses the
workflow file and job scripts from that ref, pinned at dispatch time (a later
push does not affect a running dispatch).

Praktika caches by job digest, so a job whose digest is unchanged is skipped and
its artifacts are reused. Basing a validation branch on a commit whose build
already ran turns a 1-2h build into a cache hit.

## Writing a job script that reports properly

A job that writes its own `ci/tmp/result_<normalized_job_name>.json` must get
three things right, and praktika is silent about all three:

1. **The `name` inside the file must equal the job name.** The workflow report is
   updated by `Result.update_sub_result`, which matches the job's placeholder
   entry *by name* and silently keeps the placeholder when nothing matches - the
   job's status, sub-results and artifact links are then dropped without a
   warning. Read the name from `_Environment.get().JOB_NAME`; `JOB_NAME` is not
   exported into the job's docker container.
2. **Statuses are uppercase**: `OK`, `FAIL`, `ERROR`, `SKIPPED`, `XFAIL`, `XPASS`
   (`Result.Status`). `"success"`/`"failure"` are not `is_ok()` and render as
   "completed but unknown".
3. **The GitHub job conclusion comes from the script's exit code**
   (`res = run_code == 0` in `ci/praktika/runner.py`); the result file only drives
   the HTML report and CI DB. A script that writes `FAIL` and exits 0 produces a
   green job with a red report. End such scripts with
   `[ "$OVERALL_STATUS" = "OK" ]`.

Other things worth knowing:

- Add every file the job depends on to `digest_config.include_paths`, otherwise
  editing a helper script does not invalidate the cache and the old result is
  reused.
- Secrets reach a dockerized job only if they are in the workflow's `secrets` AND
  passed through with `+-e NAME` in `run_in_docker`.
- After changing anything under `ci/workflows/` or `ci/defs/`, regenerate the
  GitHub workflow files: `PYTHONPATH=./ci:. python3 -m praktika yaml`.

## SQLancer jobs specifically

`ci/jobs/sqlancer_job.sh` (+ `ci/jobs/scripts/sqlancer_failures.py`,
`sqlancer_notify.py`) and `ci/jobs/sqlancer_pp_job.sh`, image
`ci/docker/sqlancer-test`, workflow `ci/workflows/nightly_sqlancer.py`.

- The fuzzer's own console output is deliberately filtered to one progress line
  per ~5 minutes; the full output, the server logs, one log per finding and
  `failures/analysis.txt` + `failures/findings.json` are attached to the report.
- Findings are deduplicated into distinct failures by
  (exception class + ClickHouse error code, reporting oracle, first sqlancer stack
  frame). Start triage from `analysis.txt`, not from the job log.
- The job fails on any finding, on a sanitizer report or `<Fatal>` in the server
  log, on a dead server, and on a run that produced no statistics.
- `SQLANCER_TIMEOUT_SECONDS`, `SQLANCER_REF`, `SQLANCER_BUILD_AT_RUNTIME`,
  `SQLANCER_MAX_DISTINCT_FAILURES` and `SQLANCER_SERVER_LOG_LEVEL` override the
  defaults for a one-off run.
