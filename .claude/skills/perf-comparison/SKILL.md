---
name: perf-comparison
description: Evaluate ClickHouse performance test results from existing CI/dashboard data or local perf.py runs. Use to check PR performance changes, run local performance tests, compare with history, assess flakiness, inspect coverage/PR relevance, and summarize whether a result is actionable.
argument-hint: "<PR number | performance.ci URL | runId | tests/performance/*.xml> [test/query/metric/arch]"
disable-model-invocation: false
allowed-tools: Bash, Read, Grep, Glob, WebFetch
---

# ClickHouse Performance Comparison

Use this skill whenever the goal is to answer:

> Is this ClickHouse performance result real, flaky/noisy, already known from history, or worth deeper investigation?

The evidence may come from existing performance CI/dashboard data, a local `perf.py` run, or both. Do not treat this as two separate modes; use whichever evidence is available and add missing evidence only when it changes the decision.

## Core principles

1. **Keep the current accepted evidence separate from stale exploratory data.** If a newer rerun/confidence/history check supersedes an older table, say so and show the newer numbers.
2. **Do not call a regression real from one changed row.** Check repeated PR runs, confidence, master/history trend, and test stability.
3. **Do not dismiss a regression as flaky without evidence.** Show the history/trend or repeated-run data that justifies the verdict.
4. **Do not emit opaque dashboard internals as evidence.** Raw strings like `M1:downgrade, M2:neutral` are not report-ready; translate confidence into tier/reason and named evidence checks.
5. **Metric, arch, query index, and run identity must match.** Do not mix ARM with AMD or `client_time` with CPU/real/memory metrics.
6. **Coverage/PR-diff overlap is supporting evidence, not proof.** It raises or lowers plausibility but does not replace measurements.
7. **Local runs are validation evidence, not a substitute for CI history.** Local setup differences must be reported.
8. **Never file issues, comment on PRs, or edit source unless explicitly asked.**

## Useful helper scripts in this skill

```bash
# List performance.ci runs for a PR.
python3 scripts/perf_api.py runs --pr 104350

# Inventory a PR first. This prints a small summary layer, then separates changed slowdowns,
# changed speedups, and high-noise rows.
python3 scripts/perf_api.py pr-inventory --pr 104350 --limit 20 --with-confidence

# 30-day master status classification via play.clickhouse.com default.checks.
python3 scripts/perf_api.py master-checks --pr 104350 --limit 50

# Inspect raw CI metric TSVs only when dashboard/API cannot provide required artifact-level data.
python3 scripts/perf_api.py tsv-inventory --tsv pr_104350_amd.tsv pr_104350_arm.tsv --limit 20

# Summarize a run's changed metrics.
python3 scripts/perf_api.py changes --run-id "$RUN_ID" --with-confidence

# Inspect one query with trend/history/coverage/flamegraph diff.
python3 scripts/perf_api.py query \
  --run-id "$RUN_ID" \
  --test aggregation_in_order_2 \
  --query-index 4 \
  --metric client_time \
  --arch arm \
  --with-confidence --with-trend --with-history --with-coverage --with-flamegraph-diff

# Compare one test/query/metric across all dashboard runs for a PR.
python3 scripts/perf_api.py pr-query-history \
  --pr 104350 \
  --test aggregation_in_order_2 \
  --query-index 4 \
  --metric client_time \
  --arch arm

# Parse local tests/performance/scripts/perf.py output.
mkdir -p tmp
python3 scripts/parse_perf_py.py tmp/perf-output.tsv
```

References:

- `references/ci-api.md` — performance.ci API and dashboard workflow.
- `references/local-perf.md` — local `perf.py`, optional server startup, datasets, and profiling.
- `references/verdict-rules.md` — classification rules and reporting checklist.

## Data source priority

Use the richest reliable source for each question:

1. **performance.ci API/dashboard first** for current PR runs, full changed-row inventory, confidence, query details, trend/history charts, coverage overlap, and flamegraph-diff links.
2. **ClickHouse Play HTTP (`default.checks`)** for 30-day master status counts: how often the same test/query appeared `slower`, `faster`, or `unstable` on master. Use `master-checks` for this.
3. **CI artifacts/logs** when dashboard/API cannot provide root-cause data: server logs, job logs, raw trace logs, raw profile-event TSVs, exact binary revision/build ID, or complete artifact-manifest details.
4. **Local `perf.py`** only when explicit old/new binaries are provided and local validation is useful; report it as non-CI-equivalent.

## Standard performance.ci API

```bash
BASE="https://performance.ci.clickhouse.com/api/v1"
```

Find PR runs:

```bash
PR=104350
curl -fsS "$BASE/runs?q=$PR" | jq .
```

Get run overview:

```bash
curl -fsS "$BASE/runs/$RUN_ID?metrics=client_time,real_time,cpu_time,memory" | jq .
```

Get query detail:

```bash
curl -fsS "$BASE/runs/$RUN_ID/tests/$TEST/queries/$QUERY_INDEX?metrics=client_time,real_time,cpu_time,memory" | jq .
```

Get trend/history:

```bash
curl -fsS "$BASE/runs/$RUN_ID/tests/$TEST/trend?metric=$METRIC&queryIndex=$QUERY_INDEX&arch=$ARCH" | jq .
curl -fsS "$BASE/history/$TEST/$QUERY_INDEX?metric=$METRIC&arch=$ARCH" | jq .
```

Get PR coverage overlap for a test:

```bash
curl -fsS "$BASE/runs/$RUN_ID/tests/$TEST/coverage?fileSet=intersecting" | jq .
```

Open the UI flamegraph page first:

```text
https://performance.ci.clickhouse.com/runs/$RUN_ID/tests/$TEST/queries/$QUERY_INDEX
```

Use the raw flamegraph-diff API only for machine-readable sample deltas:

```bash
curl -fsS "$BASE/runs/$RUN_ID/tests/$TEST/queries/$QUERY_INDEX/flamegraph-diff?metric=$METRIC&arch=$ARCH&traceType=CPU" | jq .
```

API caveats observed in live testing:

- `unstableQueries[]` can contain high-noise/below-threshold rows even when the summary card says `Unstable queries: 0`; do not treat them as actionable changed rows unless they are over threshold.
- `trend.selectedRunPoint.value` may be a zero marker; use query-detail old/new values for the measurement.
- `coverage?fileSet=intersecting` may omit `totals.intersectingFiles` when there are zero intersecting files; use `len(files)` as the fallback.
- Flamegraph endpoints can return 404 or no frames; missing flamegraph data is absence of evidence, not proof of noise.

## Evidence ladder

For each suspicious change, collect as much of this ladder as needed:

1. **Complete changed-row inventory, including improvements**: before picking representative examples, list changed slowdowns, changed improvements/speedups, and high-noise/unstable-threshold rows separately for the relevant PR/run/arch. Start with `python3 scripts/perf_api.py pr-inventory --pr <PR> --with-confidence`. The inventory must begin with a small generated summary layer: **Top likely signal**, **Top likely noise**, and **Top improvements**. Changed-row tables must separate signal evidence from noise/uncertain evidence. Do not mix high-noise rows into changed slowdown/speedup tables. Use CI artifacts only when dashboard/API cannot provide required raw data.
2. **Changed row details**: test, query, metric, arch, old/new, diff, threshold.
3. **Repeated PR runs**: did the same query/metric/arch change repeatedly over several dashboard runs?
4. **Confidence data**: report confidence tier/reason and named checks (`Raw Sample Evidence`, `History Adaptive Threshold`, `Recent Master Variation`, etc.). Never leave raw `M1/M2/...` codes unexplained.
5. **Master/history trend**: report period, all-history p05/p25/p50/p75/p95, recent-window p05/p50/p95, and selected old/new values versus p50/p95. Do not use only min/max/median/last.
6. **Master status counts when needed**: run `python3 scripts/perf_api.py master-checks --pr <PR>` to query `play.clickhouse.com default.checks` over master-only runs and classify rows as new/rare/flaky/unstable/fixes-known-regression. The output should be grouped by signal vs noise/uncertain evidence and link to the test page with the query selected.
7. **Related metrics**: do CPU/real/client/memory move consistently or contradict each other?
8. **Coverage/PR relation**: did the PR touch code covered by this test? does the query text match the changed subsystem?
9. **Flamegraph/profile**: if available, does a stack/function explain the changed metric?
10. **Artifacts/logs for root cause**: if dashboard/API cannot answer why, download CI artifacts for server logs, trace logs, raw profile events, SVG flamegraphs, and exact binary/build metadata.
11. **Local reproduction**: if needed, run `perf.py` against old/new binaries with isolated servers.

Stop when the verdict is clear; do not gather expensive evidence just to decorate a report. Exceptions: always inventory improvements as well as regressions; for large PRs or many changed rows, do not stop after a few representative examples; produce a portfolio/ranking summary first. Avoid repetitive all-run top tables unless the caller asks for verbose output or rerun history is central to the conclusion.

## Dashboard run stability over multiple days

For a PR, inspect all runs returned by:

```bash
curl -fsS "$BASE/runs?q=$PR" | jq '.items[] | {runId: .identity.runId, time: .identity.runTime, changed: .changedQueries, slowdowns: .slowdownQueries, speedups: .speedupQueries}'
```

For a specific query:

```bash
python3 scripts/perf_api.py pr-query-history --pr "$PR" --test "$TEST" --query-index "$QUERY_INDEX" --metric "$METRIC" --arch "$ARCH"
```

The output must include performance.ci UI links and an oldest→newest diff chart. A missing old run endpoint is `not available`; do not turn an HTTP status into an investigation conclusion.

Interpretation:

- Appears in every/most PR runs with similar direction: stronger signal.
- Appears once, disappears later: likely noise or needs rerun.
- Flips direction: unstable or measurement-sensitive.
- Appears only on one architecture: still can be real, but must be reported as arch-specific.

## Coverage and PR-diff relevance

Fetch coverage overlap:

```bash
curl -fsS "$BASE/runs/$RUN_ID/tests/$TEST/coverage?fileSet=intersecting" | jq .
```

Use:

- `totals.intersectingFiles > 0`: PR touched files executed by the perf test.
- `files[].path`, `files[].patch`, `coverageRanges`: identify touched covered lines.

If coverage is unavailable, fall back to:

```bash
gh pr diff "$PR" --name-only
gh pr diff "$PR"
```

Relate query to changes:

- aggregation query + aggregate/Aggregator pipeline changes: plausible.
- join query + analyzer/join/filter changes: plausible.
- formatting/parsing query + IO/read/write helper changes: plausible.
- no overlap and unrelated files: lower confidence, but not automatic dismissal.

## Local performance tests

If the user wants to run a test locally or validate a patch, use ClickHouse `perf.py`.

Print queries first:

```bash
tests/performance/scripts/perf.py --print-queries tests/performance/$TEST.xml
```

Compare two already-running servers:

```bash
mkdir -p tmp
tests/performance/scripts/perf.py \
  --host 127.0.0.1 127.0.0.1 \
  --port ${OLD_TCP_PORT:-9000} ${NEW_TCP_PORT:-9001} \
  --runs 7 \
  tests/performance/$TEST.xml | tee tmp/${TEST}.perf.tsv
```

Run one query index:

```bash
mkdir -p tmp
tests/performance/scripts/perf.py \
  --host 127.0.0.1 127.0.0.1 \
  --port ${OLD_TCP_PORT:-9000} ${NEW_TCP_PORT:-9001} \
  --runs 7 \
  --queries-to-run "$QUERY_INDEX" \
  tests/performance/$TEST.xml | tee tmp/${TEST}_${QUERY_INDEX}.perf.tsv
```

Parse output:

```bash
python3 scripts/parse_perf_py.py tmp/${TEST}_${QUERY_INDEX}.perf.tsv
```

If servers are not running and the user wants help, use `references/local-perf.md` and optionally `scripts/local_servers.sh`. Prefer release/static builds. Do not compare debug/sanitizer builds unless the user explicitly wants that.

Local binary contract:

- `scripts/local_servers.sh` requires explicit local paths: `OLD_CLICKHOUSE NEW_CLICKHOUSE [WORKDIR]`.
- If default ports are busy, set `OLD_TCP_PORT`, `NEW_TCP_PORT`, `OLD_HTTP_PORT`, `NEW_HTTP_PORT`.
- If paths are missing, ask the user/caller for the old/base and new/candidate binary paths. A skill invocation is just an instruction context; there is no magic binary provider, so ask the human or calling agent rather than guessing.
- Do not silently download or build binaries.
- If the user does not have binaries, suggest exact Praktika CI artifacts and ask for approval before downloading.

Praktika CI artifact lookup for performance-style release binaries:

```bash
# First get PR/run identity. Use identity.newSha for candidate and identity.oldSha for base/reference.
python3 scripts/perf_api.py runs --pr "$PR"

# Candidate PR binary from Praktika artifact report.
PR=<pr-number>
SHA=<candidate-sha>
ARCH=arm        # arm or amd, matching the run/result being reproduced
JOB="build_${ARCH}_release"
REPORT="https://clickhouse-builds.s3.amazonaws.com/PRs/${PR}/${SHA}/${JOB}/artifact_report_${JOB}.json"
curl -fsS "$REPORT" | jq -r '.build_urls[] | select(endswith("/clickhouse"))'

# Exact master/reference binary.
SHA=<reference-sha>
URL="https://clickhouse-builds.s3.amazonaws.com/REFs/master/${SHA}/${JOB}/clickhouse"
curl -sfI "$URL"
```

If the user approves a URL, download it to a named path and `chmod +x`; then pass those paths to `local_servers.sh`. Record binary path, SHA, architecture, build job, source URL, and `version()/buildId()` output in the report. Avoid non-SHA-pinned `master/amd64/clickhouse` or `master/aarch64/clickhouse` fallbacks for validation unless the user explicitly accepts them.

## Local result interpretation

`perf.py` prints important lines like:

```text
report-threshold <relative_threshold>
diff <query_index> <old_median> <new_median> <relative_diff> <pvalue>
```

Treat a local result as meaningful when:

- `abs(relative_diff)` exceeds the report threshold.
- `pvalue <= 0.05`.
- enough runs were collected.
- repeat runs agree if the result is borderline.
- machine load/config/dataset differences are not obvious confounders.

## Verdict labels

Use one of:

- `real regression`
- `likely noise`
- `needs rerun`
- `unstable test`
- `real improvement`
- `local-only evidence`
- `not enough evidence`

## Required report format

```md
## Performance verdict

Short verdict: <label>
Confidence: <high | medium | low>

## Evidence checked

- Run/API URLs:
- Test/query/metric/arch:
- Old/new/diff/threshold:
- Repeated PR runs:
- Confidence:
- History/trend:
- Coverage/PR relation:
- Flamegraph/profile: exact UI query-page link plus top sample deltas; raw API link only as backup, or explicit `not available` / `no frames returned`.
- Local perf.py, if any:

## Changed metrics

<table>

## Why this verdict

Short explanation that distinguishes current accepted evidence from stale/exploratory measurements.

## Recommended next step

- no action
- rerun performance CI
- run local perf.py
- inspect flamegraph hotspot
- inspect PR code around covered files
- bisect / targeted local validation
```

## Common mistakes to avoid

- Showing stale candidate measurements under a newer rerun/history verdict.
- Calling a PR regression real without checking repeated PR runs and history.
- Dismissing as flaky without dashboard/history evidence.
- Mixing metrics or architectures.
- Treating coverage overlap as proof of causality.
- Treating local-only results as CI-confirmed.
- Hiding changed rows that CI marked as changed.
