---
name: perf-report
description: Analyze CI performance comparison reports for a ClickHouse PR. Lists all regressions and improvements, cross-references with master history to distinguish real changes from flaky tests.
argument-hint: "<PR-number or CI-report-URL>"
disable-model-invocation: false
allowed-tools: Bash, Read, Grep, Glob, WebFetch
---

# Performance Report Analysis Skill

## Arguments

- `$0` (required): PR number (e.g. `99474`) or a CI report URL

## Steps

### 1. Fetch performance data

Use the `fetch_perf_report.py` tool to get TSV data for both architectures:

```bash
python3 .claude/tools/fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/$PR" --arch amd --tsv
python3 .claude/tools/fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/$PR" --arch arm --tsv
```

If a direct CI report URL is given instead of a PR number, use it directly.

Extract all changed queries (column 10 == 1 means the change exceeds the threshold):
- Column 1: test name
- Column 2: query index
- Column 4: batch
- Column 5: old time
- Column 6: new time
- Column 8: ratio (e.g. 1.5 = 1.5x)
- Column 10: 1 if changed, 0 if within threshold
- Column 12: "slower" or "faster"
- Column 13: query text

### 2. List ALL changes

Present the **complete, unfiltered** list of all changes above 1.10x, sorted by magnitude, for both architectures separately. Use tables with columns: Magnitude, Direction, Test, Query#, Batch.

Do NOT summarize, collapse, or hide entries. Do NOT dismiss anything as "noise" or "not actionable" without evidence. Every entry must be visible.

### 3. Cross-reference with master history

**This step is MANDATORY.** For every test that shows as slower or faster above 1.10x, check the public CI database to determine if this is a known flaky test or a genuine change introduced by the PR. Do NOT skip this step or use alternative approaches (like manually fetching perf reports from other PRs).

Query the `default.checks` table on `play.clickhouse.com` with `user=explorer`:

```bash
clickhouse client --format PrettyCompactNoEscapes --host play.clickhouse.com --user explorer --secure --query "
SELECT
    replaceRegexpOne(test_name, '::(new|old)$', '') AS test,
    countIf(test_status = 'slower') AS slower_count,
    countIf(test_status = 'faster') AS faster_count,
    countIf(test_status = 'unstable') AS unstable_count,
    count() AS total_runs
FROM default.checks
WHERE pull_request_number = 0
  AND check_name LIKE '%Performance%amd%'
  AND check_start_time >= now() - INTERVAL 30 DAY
  AND test_name IN (
    'norm_distance #2::new',
    'array_sort #0::new'
  )
GROUP BY test
ORDER BY slower_count DESC, test
"
```

Run **one query per architecture** — use `'%Performance%amd%'` for x86 and `'%Performance%arm%'` for ARM.

**Critical details:**
- **Host:** `play.clickhouse.com`, **user:** `explorer` (NOT `play`)
- **Table:** `default.checks` (NOT `perftest` or other tables)
- `pull_request_number = 0` filters to master-only commits (no PR noise)
- Test names in the DB have `::new` and `::old` suffixes — always query with `::new`
- Include ALL changed tests in a single IN clause to minimize round-trips

### 4. Classify each change

For each test, classify based on the master history:

- **Flaky on master**: appears as slower in >1% of master runs, or has many unstable entries. Note the count (e.g. "8/685 runs").
- **New in this PR**: 0 slower (or faster) appearances on master in the last 30 days with a meaningful number of total runs (>50). This change was first observed in this PR.
- **Rarely on master**: 1-2 appearances out of hundreds. Treat as borderline — note it but don't dismiss.
- For "faster" results: check if the test was previously *slower* on master (meaning this PR fixes it).

### 5. Present the verdict

Present the final classification in a table per architecture:

| Magnitude | Test | Master slower/total (30d) | Verdict |
|---|---|---|---|

Classify as:
- **Flaky** — frequently appears on master, dismiss
- **Unstable** — high unstable count on master, dismiss
- **New in this PR — investigate** — regression not seen on master before
- **New in this PR — improvement** — speedup not seen on master before, or fixes a previously-slower test
- **Rarely on master** — borderline, note it

### 6. Summary

After the tables, provide a brief summary:
- Count of genuine regressions (never on master) per architecture
- Count of genuine improvements per architecture
- Count of flaky/dismissed entries
- Call out any extreme outliers (>2x) explicitly regardless of flaky status

### 7. Deep-dive: accessing CI logs (when asked)

When the user asks to investigate a specific regression further, download and analyze the CI artifacts.

**Get artifact links** using the `fetch_ci_report.js` tool with `--links`:

```bash
node .claude/tools/fetch_ci_report.js "<CI-report-URL-for-specific-batch>" --links
```

This will show `logs.tar.zst`, `job.log.zst`, `all-query-metrics.tsv`, `report.html`, etc.

**Download and extract server logs:**

```bash
curl -sS "<logs.tar.zst-URL>" -o tmp/perf_logs.tar.zst
tar -I zstd -tf tmp/perf_logs.tar.zst  # list contents
tar -I zstd -xf tmp/perf_logs.tar.zst -C tmp/ ./right/server.log  # PR binary
tar -I zstd -xf tmp/perf_logs.tar.zst -C tmp/ ./left/server.log   # master binary
```

- `right/server.log` = PR binary (the "new" version)
- `left/server.log` = master binary (the "old" version)

**Analyze the query execution** by finding the query ID in the server log:

```bash
# Find the query and its timing
grep "math.query2" tmp/right/server.log | grep -E "Aggregated|Read.*rows.*sec"
```

The perf framework uses query IDs like `{test_name.query{N}.run{M}}` (e.g. `math.query2.run0`). Look at:
- `Aggregated ... in X sec` — actual compute time
- `executeQuery: Read N rows ... in X sec` — total query time
- `TCPHandler: Processed in X sec` — wall clock including network

**Compare both servers** during the same time window to see if the machine was under load or if only the PR binary was slow. Check for:
- Background activity (merges, flushes, system log writes)
- Errors or warnings
- Whether the slowdown is in UserTime (CPU-bound) or wall clock (I/O/contention)

**Check the git hash** of the binary that actually ran:

```bash
grep "Starting ClickHouse" tmp/right/server.log | head -1
```

This shows the exact revision, build ID, and PID. Compare with what you expect — the CI perf test may use a different binary than the latest commit if the build was cached.

### 8. Deep-dive: trace log profiling (flamegraphs)

The `logs.tar.zst` archive contains `right-trace-log.tsv` and `left-trace-log.tsv` — these are exports of `system.trace_log` from each server, containing CPU and real-time stack samples for every query.

**Extract trace logs:**

```bash
tar -I zstd -xf tmp/perf_logs.tar.zst -C tmp/ ./right-trace-log.tsv ./left-trace-log.tsv
```

**Key columns** (TSV format, header in row 1, types in row 2, data from row 3):
- Column 7: `trace_type` — `CPU` (sampled CPU time), `Real` (wall clock), `Memory`, etc.
- Column 11: `query_id` — matches the `{test.queryN.runM}` pattern from server logs
- Column 19: `symbols` — comma-separated list of function names (leaf first), wrapped in `['...']`

**Find the hotspot for a specific query** — extract CPU traces and count leaf functions:

```bash
grep "math.query2" tmp/right-trace-log.tsv | awk -F'\t' '$7 == "CPU" {print $19}' | \
  sed "s/\[//g; s/\]//g; s/'//g" | \
  awk -F',' '{print $1}' | \
  sort | uniq -c | sort -rn | head -20
```

This gives a flat profile of where CPU time is spent, similar to `perf report`. Compare left (master) vs right (PR) to see what changed.

**Build collapsed stacks for flamegraph visualization:**

```bash
grep "math.query2" tmp/right-trace-log.tsv | awk -F'\t' '$7 == "CPU" {print $19}' | \
  sed "s/\[//g; s/\]//g; s/','/;/g; s/'//g" | \
  sort | uniq -c | awk '{print $2, $1}' > tmp/math2_right.collapsed
```

The resulting `.collapsed` file can be processed with `flamegraph.pl` or the `analyze-assembly.py --perf-map` tool.

**This is the fastest way to identify the root cause of a regression.** Example: for a 31x `exp10` regression, the trace log immediately showed `modf` consuming 577/1200 CPU samples on the PR binary vs 28/60 on master — pinpointing the exact function responsible without needing to reproduce locally.

**The archive also contains pre-built SVG flamegraphs** for queries the framework selected for detailed analysis:

```bash
tar -I zstd -tf tmp/perf_logs.tar.zst | grep "\.svg"
```

These come in `.left.svg` (master), `.right.svg` (PR), and `.diff.svg` (differential) variants, for both `CPU` and `Real` time. Not all queries get flamegraphs — only those the framework considers interesting.

### 9. Deep-dive: profile events from raw TSV

The archive contains per-query raw metric data in `analyze/tmp/{test}_{queryN}.tsv`. Each row is one run, with an array of all ProfileEvents (counters like `UserTimeMicroseconds`, `OSCPUVirtualTimeMicroseconds`, `RealTimeMicroseconds`, etc.).

```bash
tar -I zstd -xf tmp/perf_logs.tar.zst -C tmp/ ./analyze/tmp/math_2.tsv
```

The `all-query-metrics.tsv` file (linked from `--links` output) contains the processed comparison data with old/new values, ratios, and thresholds for every metric of every query. The `fetch_perf_report.py` tool already parses this, but the raw file has all metrics, not just `client_time`.

### Important: use unique download paths

When analyzing multiple batches or PRs, use unique directory names to avoid overwriting:

```bash
mkdir -p tmp/batch1_amd tmp/batch5_amd
curl -sS "<batch1-logs-url>" -o tmp/batch1_amd/logs.tar.zst
curl -sS "<batch5-logs-url>" -o tmp/batch5_amd/logs.tar.zst
tar -I zstd -xf tmp/batch1_amd/logs.tar.zst -C tmp/batch1_amd/
```

## Rules

- **Never dismiss a regression without checking master history first.** Do not call anything "noise" or "not actionable" based on intuition alone.
- **Show all data.** The user wants the full picture, not a filtered summary.
- **Both architectures matter.** AMD runs on Intel Sapphire Rapids (m7i.4xlarge), ARM runs on Graviton 4 (m8g.4xlarge). Regressions on one but not the other are still real.
- **A test appearing in multiple CI runs of the same PR is a strong signal** even if it also occasionally appears on master.
- **Do not look at the PR code or run local benchmarks** unless explicitly asked. This skill is purely about CI report analysis.
