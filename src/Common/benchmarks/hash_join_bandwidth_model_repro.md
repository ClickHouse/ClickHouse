# Reproducing the NPHJ benchmark as a real ClickHouse query

The NPHJ ("non-partitioned hash join") competitor in `hash_join_bandwidth_model` drives the real
`ConcurrentHashJoin` (`parallel_hash`), so its numbers *should* match a real query of the same
shape. They do — but only if the SQL query is genuinely equivalent to what the benchmark measures.
This document records the verified-equivalent recipe and, more importantly, the mistakes that were
made (twice, in separate sessions) when trying to compare the two, so future runs do not repeat
them.

Everything below was validated on a 96-core aarch64 machine at the shape
`N_b = 2^30` build rows x `N_p = 2^32` probe rows, 1 payload column per side (16 B/row),
32 GiB hash table. With the recipe followed exactly, the real query and the benchmark agreed to
within ~2-5% end-to-end, ~10% on the match phase, and ~8% on the gather phase, in two independent
validation sessions.

## What the benchmark actually measures

`hash_join_bandwidth_model --join-nb <N_b> --join-np <N_p> --algo nphj` drives, per run:

- **build**: concurrent `addBlockToJoin` from all threads + `onBuildPhaseFinish` bucket merge.
- **probe+gather**: `joinBlock` from all threads, with **every output block fully materialized
  and drained** (`drainJoinResult` iterates `JoinResult::next`), then dropped. Both sides'
  payload columns are part of the output.
- **teardown** (reported separately, not part of `JoinStats::total`): destruction of the
  `ConcurrentHashJoin`, i.e. the parallel `clearAndShrink` of the shared two-level map. At a
  32 GiB table this is ~2 s of real time.

Input shape (see `runSingleJoin`):

- Build keys: the **exact key space `[0, N_b)`, each value exactly once, in shuffled order**
  (`uniqueKeys` -> `shuffledIndex`, a bijective mix). Duplicate-free, so `onBuildPhaseFinish`
  promotes `All` to `RightAny` and every probe is a point lookup emitting exactly one row.
- Probe keys: a **shuffled exact permutation over `[0, N_b)`** (`probePermutationKeys`) — every
  build key appears exactly `N_p / N_b` times, in random order. Hit rate 1.0 by default.
- Columns: `UInt64` key + one `UInt64` payload per side by default
  (`--build-payload-columns` / `--probe-payload-columns`).
- Threads: all cores by default (`--threads`).
- Statistics warm-up: the bench passes a `StatsCollectingParams` key, so run 0 builds cold and
  populates the size-hint cache, and later runs preallocate the maps — the steady state of a
  repeatedly-executed real query (`collect_hash_table_stats_during_joins`, default `true`).
  **Compare steady-state runs (run 1+), not run 0.**

## The verified-equivalent SQL

```sql
-- One-time setup (in-memory tables; key + payload, matching the benchmark's 16 B rows).
CREATE TABLE build_side (k UInt64, payload UInt64) ENGINE = Memory;
CREATE TABLE probe_side (k UInt64, payload UInt64) ENGINE = Memory;

-- Build side: dense unique key space [0, N_b), like the benchmark's shuffled permutation.
INSERT INTO build_side
SELECT number, number FROM numbers(1073741824)
SETTINGS max_threads = 96, max_insert_threads = 96;

-- Probe side: RANDOM-ORDER keys covering [0, N_b). `cityHash64(number) % N_b` approximates the
-- benchmark's shuffled exact permutation (see "residual differences" below).
INSERT INTO probe_side
SELECT cityHash64(number) % 1073741824, number FROM numbers(4294967296)
SETTINGS max_threads = 96, max_insert_threads = 96;

-- The measured query. Run it at least twice; compare the steady-state (2nd+) runs.
SELECT probe.payload, build.payload
FROM probe_side AS probe
INNER JOIN build_side AS build ON probe.k = build.k
FORMAT Null
SETTINGS
    join_algorithm = 'parallel_hash',
    enable_join_runtime_filters = 0,
    max_threads = 96;
```

Phase-level comparison comes from the same `ProfileEvents` the benchmark prints
(`match/thr`, `gather/thr`, `dispatch/thr` are these counters divided by the thread count):

```sql
SYSTEM FLUSH LOGS;
SELECT
    query_duration_ms,
    round(ProfileEvents['HashJoinProbeMatchMicroseconds']  / (96 * 1000), 1) AS match_per_thr_ms,
    round(ProfileEvents['HashJoinProbeGatherMicroseconds'] / (96 * 1000), 1) AS gather_per_thr_ms
FROM system.query_log
WHERE type = 'QueryFinish' AND query LIKE '%probe.payload%'
ORDER BY event_time DESC;
```

## The four traps (each one silently breaks the comparison)

### 1. `SELECT count()` — column pruning makes the gather free

A bare `SELECT count() FROM ... INNER JOIN ...` needs **zero output columns**; the analyzer
prunes both sides' payloads and the gather phase (`HashJoinResult::next`) does almost nothing.

Measured at the reference shape: gather ~**5 ms/thr** with `count()` vs ~**3500 ms/thr** with
payload columns selected — a 700x difference in the phase, several seconds end-to-end. The
benchmark always materializes every payload column, so `count()` is not comparable to it.
This exact mistake was made independently in two sessions; it is the reason this document exists.

### 2. Sequential probe keys — unrealistic gather locality

`number % N_b` as the probe key looks harmless but produces **sequentially increasing keys**, so
consecutive output rows gather from consecutive build-side stored-block positions. The gather
becomes a near-sequential copy instead of the random access the benchmark (and any realistic
join) performs.

Measured: gather ~**375 ms/thr** with sequential keys vs ~**3800 ms/thr** with
`cityHash64(number) % N_b`; end-to-end ~4.5 s vs ~9 s. Note that selecting the payload columns
does *not* protect against this — trap 1 and trap 2 are independent and their fixes must both
be applied.

### 3. Runtime filters — a single-threaded cost the benchmark does not model

`enable_join_runtime_filters` defaults to `true`. `BuildRuntimeFilterTransform` runs on **one
thread** and, for a 1B-row build side, took ~13 s — dominating a ~19 s query in which the join
itself was ~5 s. The benchmark has no analogue of this transform, so disable it
(`enable_join_runtime_filters = 0`) when validating the join model. (Its cost is worth knowing
about, but it is a separate phenomenon from hash join performance.)

### 4. Teardown accounting

The benchmark reports teardown separately because production tears the join down at pipeline
destruction, after the last output block. When comparing end-to-end times, compare the client
wall time against the benchmark's **build + probe + teardown** sum (~2 s of teardown at a
32 GiB table). `system.query_log.query_duration_ms` is measured after the pipeline reset and
includes teardown; the client-observed time may or may not, depending on protocol and version —
at the reference shape the two differed by roughly the teardown time.

## Residual differences that remain after following the recipe

- **match/thr ~7-10% higher in SQL**: `cityHash64(number) % N_b` is a multinomial approximation
  of the benchmark's exact permutation — key frequencies vary around `N_p / N_b` and a fraction
  `e^-(N_p/N_b)` of build keys is never probed (~2% at ratio 4, ~37% at ratio 1). This shifts
  hash-collision and cache behavior slightly; output row counts still equal `N_p` because the
  duplicate-free build side promotes the join to `RightAny` point lookups.
- **Run-to-run noise of +-10-15%** at billion scale (first-touch page faults, memory-bandwidth
  contention). Use medians of several steady-state runs.
- **Block size**: the benchmark generates `DEFAULT_BLOCK_SIZE`-row chunks; `INSERT ... SELECT`
  into a `Memory` table produces similar but not identical block boundaries. Not observed to
  matter at this shape.

## Reference numbers (96-core aarch64, N_b = 2^30, N_p = 2^32, 1 payload column/side)

| metric | benchmark NPHJ (steady state) | real query (recipe above) |
|---|---|---|
| match/thr | ~1830 ms | ~2020 ms |
| gather/thr | ~3500 ms | ~3800 ms |
| build + probe | ~7.0 s | — |
| teardown | ~1.9 s | — |
| end-to-end | ~9.0 s | ~8.8-9.1 s (client), ~10.7-11.0 s (`query_duration_ms`) |

The corresponding benchmark invocation:

```bash
./build/reldeb/src/Common/benchmarks/hash_join_bandwidth_model \
    --algo nphj --join-nb $((1<<30)) --join-np $((1<<32)) --runs 3
```

(`--algo nphj` also avoids the RPHJ scatter's 2^32-1 rows-per-side limit at this probe size.)
