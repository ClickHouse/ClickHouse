---
name: instrument-profile
description: Measure exact per-call timing inside a running ClickHouse server using SYSTEM INSTRUMENT (LLVM XRay), without rebuilding or restarting. Use when aggregate ProfileEvents are not enough to explain a latency difference, when a sampling profiler shows nothing because threads are blocked rather than on-CPU, when you need call counts and durations for one specific function, or when you need to reconstruct concurrency / in-flight depth over the life of a query. Also use when someone mentions SYSTEM INSTRUMENT, XRay, system.instrumentation, or trace_type='Instrumentation'.
argument-hint: "<function-substring> [query-to-profile]"
---

# Instrument a running ClickHouse server

`SYSTEM INSTRUMENT` patches LLVM XRay hooks into the prologue/epilogue of a live
server's functions. No rebuild, no restart, no code change. When no point is added
the cost is one jump per instrumented function, so it is safe to leave available in
production.

Use it when you need **per-call intervals** — counts, durations, and start/end
timestamps for a named function. That is the gap between ProfileEvents (totals only)
and the sampling profiler (samples, no call attribution).

## Preflight

The server must be built with `ENABLE_XRAY=1`. Check by querying the table — if it
does not exist, this technique is unavailable and you need `cpu-profile` instead.

```sql
SELECT count() FROM system.instrumentation;          -- table exists => XRay build
SELECT name FROM system.build_options WHERE name LIKE '%XRAY%';
```

## Quick reference

| Action | Statement |
|---|---|
| Time a function | `SYSTEM INSTRUMENT ADD 'Namespace::func' PROFILE` |
| Log + stack at entry | `SYSTEM INSTRUMENT ADD 'Namespace::func' LOG ENTRY 'text'` |
| Inject a delay (fault injection) | `SYSTEM INSTRUMENT ADD 'Namespace::func' SLEEP ENTRY 0.5` |
| Random delay | `SYSTEM INSTRUMENT ADD '...' SLEEP ENTRY 0 1` |
| List active points | `SELECT * FROM system.instrumentation FORMAT Vertical` |
| Remove one | `SYSTEM INSTRUMENT REMOVE <id>` |
| Remove all | `SYSTEM INSTRUMENT REMOVE ALL` |

The function argument is a **substring** of the mangled/demangled name, so
`'Parquet::Reader::decodePrimitiveColumn'` matches without the full signature.
Results land in `system.trace_log` with `trace_type = 'Instrumentation'`, one row
per ENTRY and one per EXIT.

## Recipe 1 — per-function totals

`duration_nanoseconds` is populated on the `Exit` row only.

```sql
SELECT substring(splitByChar('(', function_name)[1], 1, 60) AS fn,
       count() AS calls,
       round(sum(duration_nanoseconds)/1e6, 1) AS total_ms,
       round(avg(duration_nanoseconds)/1000, 1) AS avg_us
FROM system.trace_log
WHERE query_id = {qid:String} AND trace_type = 'Instrumentation' AND entry_type = 'Exit'
GROUP BY fn ORDER BY total_ms DESC;
```

## Recipe 2 — duration histogram (find the blocking tail)

Totals hide the shape. A function whose average doubled may simply have more slow
calls rather than slower calls — that distinction usually decides the diagnosis.

```sql
SELECT multiIf(duration_nanoseconds <  100000, 'a <0.1ms',
               duration_nanoseconds < 1000000, 'b 0.1-1ms',
               duration_nanoseconds < 5000000, 'c 1-5ms', 'd >5ms') AS bucket,
       count() AS n, round(sum(duration_nanoseconds)/1e6) AS ms
FROM system.trace_log
WHERE query_id = {qid:String} AND trace_type = 'Instrumentation'
  AND entry_type = 'Exit' AND function_name LIKE '%getRangeData%'
GROUP BY bucket ORDER BY bucket;
```

## Recipe 3 — in-flight concurrency over time

The highest-value trick, and not obvious: an `Exit` row carries both the end
timestamp and the duration, so each call is an interval
`[event_time_microseconds - duration, event_time_microseconds]`. Sweeping those
intervals reconstructs **instantaneous concurrency** — how many reads, decodes, or
requests were in flight at each moment. Aggregate counters cannot show this.

```sql
SELECT toUnixTimestamp64Micro(event_time_microseconds) - intDiv(duration_nanoseconds, 1000) AS start_us,
       toUnixTimestamp64Micro(event_time_microseconds) AS end_us
FROM system.trace_log
WHERE query_id = {qid:String} AND trace_type = 'Instrumentation'
  AND entry_type = 'Exit' AND function_name LIKE '%Prefetcher::runTask%'
ORDER BY start_us
```

Then sweep `(start,+1),(end,-1)` sorted by time, tracking a running counter for the
peak, the time-weighted average, and the fraction of the span spent at each depth.
A distribution pinned at exactly the pool size means the pool is the limit; a
decaying burst means concurrency came from somewhere else (e.g. work-stealing).

## Recipe 4 — who ran what, via thread_id

`thread_name` is empty on these rows. Correlate by `thread_id` instead: a thread
appearing in two functions did both, which distinguishes a dedicated pool thread
from a consumer thread that executed work inline.

```sql
WITH t AS (
    SELECT thread_id,
           countIf(function_name LIKE '%runTask%')     AS ran,
           countIf(function_name LIKE '%getRangeData%') AS consumed
    FROM system.trace_log
    WHERE query_id = {qid:String} AND trace_type = 'Instrumentation' AND entry_type = 'Exit'
    GROUP BY thread_id
)
SELECT sumIf(ran, consumed > 0) AS ran_inline_by_consumers,
       sumIf(ran, consumed = 0) AS ran_on_pool_threads
FROM t;
```

## Gotchas

- **Short functions cannot be instrumented.** XRay only patches functions longer
  than ~200 instructions; `ADD` throws for the rest. Expect some of your chosen
  points to be rejected — pick a caller instead of asserting the callee is cold.
- **`ARGUMENTS` are the handler's, not the function's.** They supply the `LOG` text
  or the `SLEEP` seconds. There is no capture of the instrumented function's
  parameters, so you cannot tag a call with which object it operated on. If you need
  per-object attribution, correlate by timestamp against another log (e.g.
  `ReadBufferFromS3` lines with `send_logs_level=test`).
- **Observer effect is real and can hide the bug.** Instrumenting hot functions adds
  overhead that slows the fast path and can shrink the very difference you are
  measuring. Always record the wall clock of the instrumented run and compare it to
  the uninstrumented baseline; if the effect shrank, the attribution still holds but
  the magnitudes do not.
- **Lambdas inherit the enclosing function's name.** A closure defined inside
  `f()` reports as `f()::$_0`, and any callee inlined into it looks like time spent
  in `f`. A function appearing in a large share of stacks may be doing none of that
  work itself.
- **Points are server-global.** They affect every query and every user on that
  server, not your session. On a shared or production server, add the minimum set,
  and always finish with `SYSTEM INSTRUMENT REMOVE ALL`.
- **Volume.** A few hot points on one query can produce >100k `trace_log` rows.
  Always filter by `query_id`, and prefer a small, narrow query over a big one.
- **`SLEEP` is fault injection.** Useful for widening a race until it reproduces;
  disruptive on anything shared.
- `allow_introspection_functions` is only needed for `addressToSymbol`/`demangle`
  on `CPU`/`Real` traces. `Instrumentation` rows already carry `function_name`.

## Choosing between the profilers

| Symptom | Use |
|---|---|
| Want to know where CPU goes | `cpu-profile` (`trace_type='CPU'`) |
| Threads are blocked, CPU profile is empty or flat | `Real` traces, or this skill |
| Need call counts / per-call durations for one function | this skill |
| Need concurrency or in-flight depth over time | this skill, Recipe 3 |
| Need per-object attribution (which file, which range) | not available — correlate with `send_logs_level=test` logs |

## Common mistakes

- Reading `duration_nanoseconds` from `Entry` rows (it is only on `Exit`).
- Concluding "the function is a hotspot" from stack share alone when a lambda or
  inlined callee is doing the work (see gotcha above).
- Trusting instrumented magnitudes as if uninstrumented — verify against a clean run.
- Leaving points installed on a shared server after finishing.
- Reaching for this before checking whether an existing ProfileEvent already answers
  the question; per-call detail is only worth its cost when totals are ambiguous.
