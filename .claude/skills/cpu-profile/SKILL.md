---
name: cpu-profile
description: Profile a ClickHouse query using the sampling query profiler and system.trace_log. Use when the user wants to find CPU hotspots, analyze where time is spent in a query, or investigate performance bottlenecks.
argument-hint: [query_id or query text]
disable-model-invocation: false
allowed-tools: Task, Bash, Read, Grep, Glob, AskUserQuestion
---

# CPU Profile Analysis Skill

Profile a ClickHouse query using the built-in sampling query profiler (`system.trace_log`).
Collects CPU stack traces at configurable intervals and analyzes them to find hotspots.

## Arguments

- `$ARGUMENTS` (optional): Either a `query_id` to analyze existing traces, or a SQL query to execute with profiling enabled.

## Step 1 — Determine what to profile

If `$ARGUMENTS` looks like a UUID (e.g., `a1b2c3d4-e5f6-...`), treat it as a `query_id` and skip to Step 3.

If `$ARGUMENTS` is a SQL query or query description, proceed to Step 2.

If `$ARGUMENTS` is empty, ask the user:
- **Question**: "What would you like to profile?"
- Options: "Enter a query_id from a previous run", "Enter a SQL query to execute now", "Show recent slow queries from query_log"

If the user wants to see recent slow queries:
```sql
SELECT
    query_id,
    query_duration_ms,
    formatReadableSize(memory_usage) AS peak_memory,
    left(query, 120) AS query_preview
FROM system.query_log
WHERE type = 'QueryFinish'
  AND event_date >= today() - 1
  AND query_duration_ms > 1000
  AND query NOT LIKE '%system.%'
ORDER BY query_duration_ms DESC
LIMIT 20
SETTINGS allow_introspection_functions = 1
```

## Step 2 — Execute query with profiling

Generate a unique query ID and run the query with aggressive profiling settings (100us sampling = ~10,000 samples/sec).

Use `clickhouse-client` in non-interactive mode with an explicit `--query_id` to avoid any race with concurrent queries:

```bash
PROFILE_QID="cpu-profile-$(uuidgen)"
clickhouse-client --query_id "$PROFILE_QID" -q "
    SELECT ...
    SETTINGS query_profiler_cpu_time_period_ns = 100000,
             query_profiler_real_time_period_ns = 100000
"
```

Alternatively, if running interactively, parse the `Query id: <uuid>` line that `clickhouse-client` prints before each query.

After execution, verify the query completed and collect metadata:
```sql
SELECT query_id, query_duration_ms, formatReadableSize(memory_usage) AS peak_memory
FROM system.query_log
WHERE type = 'QueryFinish' AND query_id = '{query_id}'
SETTINGS allow_introspection_functions = 1
```

Wait 2 seconds for `trace_log` to flush, then proceed to Step 3.

## Step 3 — Collect and analyze trace data

Run these analyses in parallel using Task tool (3 tasks):

### Agent A — Top functions by CPU samples

```sql
SELECT
    count() AS samples,
    round(100.0 * count() / (SELECT count() FROM system.trace_log WHERE query_id = '{query_id}' AND trace_type = 'CPU'), 2) AS pct,
    demangle(addressToSymbol(trace[1])) AS function
FROM system.trace_log
WHERE query_id = '{query_id}'
  AND trace_type = 'CPU'
GROUP BY function
ORDER BY samples DESC
LIMIT 30
SETTINGS allow_introspection_functions = 1
```

### Agent B — Top stack traces (full call paths)

```sql
SELECT
    count() AS samples,
    arrayStringConcat(
        arrayMap(x -> demangle(addressToSymbol(x)), trace),
        '\n    '
    ) AS stack
FROM system.trace_log
WHERE query_id = '{query_id}'
  AND trace_type = 'CPU'
GROUP BY trace
ORDER BY samples DESC
LIMIT 15
SETTINGS allow_introspection_functions = 1
```

### Agent C — Export collapsed stacks for flamegraph

```sql
SELECT
    concat(
        arrayStringConcat(
            arrayReverse(arrayMap(x -> demangle(addressToSymbol(x)), trace)),
            ';'
        ),
        ' ',
        toString(count())
    )
FROM system.trace_log
WHERE query_id = '{query_id}'
  AND trace_type = 'CPU'
GROUP BY trace
ORDER BY count() DESC
SETTINGS allow_introspection_functions = 1
FORMAT TSVRaw
```

Save this output to `tmp/cpu_profile_{query_id}.collapsed` for optional flamegraph generation.

Also collect metadata:
```sql
SELECT
    count() AS total_samples,
    min(event_time_microseconds) AS first_sample,
    max(event_time_microseconds) AS last_sample,
    dateDiff('millisecond', min(event_time_microseconds), max(event_time_microseconds)) AS profile_duration_ms
FROM system.trace_log
WHERE query_id = '{query_id}' AND trace_type = 'CPU'
SETTINGS allow_introspection_functions = 1
```

## Step 4 — Synthesize results

Using outputs from all three agents, produce a structured report:

1. **Profile summary**: query_id, total samples, profile duration, sampling rate
2. **Top 15 CPU hotspot functions** with sample count and percentage — as a table
3. **Top 5 full stack traces** with readable formatting — show the call chain from outermost to innermost
4. **Subsystem breakdown**: Group functions into categories:
   - Query Execution (HashJoin, Aggregator, MergeSorter, etc.)
   - Expression Evaluation (ExpressionActions, functions)
   - IO (ReadBuffer, WriteBuffer, S3, disk)
   - Network (Exchange, Connection, Protocol)
   - Compression (LZ4, ZSTD, codecs)
   - Memory Management (Arena, Allocator, PODArray)
   - Optimizer (Cascades, JoinOrder, Statistics)
   - Other
5. **Actionable findings**: What's unexpectedly hot, what could be optimized
6. **Collapsed stack file location** for flamegraph generation

## Step 5 — Offer drill-down options

Ask the user:
- **"Drill into a function"**: Filter traces containing a specific function name
- **"Compare CPU vs Real time"**: Run the same analysis for `trace_type = 'Real'` to find wall-clock hotspots (IO waits, lock contention)
- **"Generate flamegraph"**: If `flamegraph.pl` is available, render SVG:
  ```bash
  flamegraph.pl --title "CPU Profile: {query_id}" --countname samples --width 1800 \
    tmp/cpu_profile_{query_id}.collapsed > tmp/cpu_flamegraph_{query_id}.svg
  ```
  Or suggest using https://www.speedscope.app with the collapsed file.
- **"Show source locations"**: Re-run with `addressToLine` for source file:line mapping:
  ```sql
  SELECT
      count() AS samples,
      demangle(addressToSymbol(trace[1])) AS function,
      addressToLine(trace[1]) AS source_location
  FROM system.trace_log
  WHERE query_id = '{query_id}' AND trace_type = 'CPU'
  GROUP BY function, source_location
  ORDER BY samples DESC
  LIMIT 30
  SETTINGS allow_introspection_functions = 1
  ```
- **"Done"**: Exit

Repeat drill-down until user selects "Done".

## Notes

- The `query_profiler_cpu_time_period_ns` setting controls sampling frequency. Default is 1,000,000,000 (1 sample/sec). Use 100,000 (100us) for detailed profiling of short queries, 1,000,000 (1ms) for longer queries.
- `trace_type = 'CPU'` counts CPU time; `trace_type = 'Real'` counts wall-clock time (includes IO waits).
- `allow_introspection_functions = 1` is required for `addressToSymbol`, `demangle`, `addressToLine`.
- The `clickhouse-common-static-dbg` package must be installed for symbol resolution.
- On ClickHouse Cloud, use `FROM clusterAllReplicas(default, system.trace_log)` to collect traces from all nodes.
- Stack traces in `system.trace_log` are stored as arrays of addresses, with index 1 being the innermost (leaf) frame.

## Examples

- `/cpu-profile` — Interactive: choose a query to profile
- `/cpu-profile a1b2c3d4-e5f6-7890-abcd-ef1234567890` — Analyze existing traces for a query_id
- `/cpu-profile SELECT count() FROM lineitem WHERE l_shipdate > '1995-01-01'` — Execute and profile a query
