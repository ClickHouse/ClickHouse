# Granular (sub-part) parallelism for primary key and skip index analysis

Date: 2026-06-09
Branch: `granular-index-analysis`
Area: `src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp`

## Problem

Index analysis — selecting which mark ranges to read via the primary key and skip
indexes — is parallelized strictly at the **part level**. In
`filterPartsByPrimaryKeyAndSkipIndexes`
(`MergeTreeDataSelectExecutor.cpp:819`), one `process_part` task is scheduled per
part into a `ThreadPool`, and `num_threads` is capped by the number of parts:

```cpp
size_t num_threads = std::min<size_t>(num_streams, parts_with_ranges.size());
if (settings[Setting::max_threads_for_indexes])
    num_threads = std::min<size_t>(num_streams, settings[Setting::max_threads_for_indexes]);
...
for (size_t part_index = 0; part_index < parts_with_ranges.size(); ++part_index)
    pool.scheduleOrThrow([&, part_index]{ process_part(part_index); }, ...);
```

Each task does the part's full analysis sequentially: `markRangesFromPKRange`
(`:960`) followed by the skip-index filtering loop (`:1011`). Consequences:

1. **Few large parts.** After heavy merging a query may face 1–8 huge parts. With
   `num_parts < num_threads`, most threads idle while a couple grind through
   millions of granules of skip-index evaluation (the expensive, IO-bound step).
   This is the dominant real-world bottleneck.
2. **Many skip indexes per part** all run on one thread, though independent.

## Goal

Make the unit of parallelism a **mark-range chunk within a part** rather than a
whole part. Both `markRangesFromPKRange` and the skip-index filtering loop run at
chunk granularity. This removes the `num_parts` ceiling and fixes the
few-large-parts imbalance.

This is "design 2": a static, all-items-scheduled-up-front shared `ThreadPool`
whose shared queue load-balances. The work-unit model is chosen so the later
"design 3" (adaptive work-stealing that re-splits an oversized in-flight chunk to
kill the tail) is a **scheduler-only** change, not a rewrite.

Non-goals (this iteration): adaptive re-splitting; per-chunk handling of the
disjunction bitset, top-K min-max, and FINAL exact mode (see fallback below).

## Design

### Work unit

```
WorkItem { size_t part_index; size_t chunk_index; MarkRanges sub_ranges; bool whole_part; }
```

`sub_ranges` is a contiguous slice of the part's **existing** `ranges.ranges`.
Chunking partitions the *existing* ranges, never a blind `[0, marks)` — a part may
arrive already restricted by sampling or parallel-replicas mark allocation. Chunk
boundaries walk the flattened mark sequence of `ranges.ranges`.

### Chunk sizing

```
total        = Σ over parts of part.getMarksCount()
chunk_marks  = clamp(total / (num_threads * K), MIN_MARKS_PER_TASK, total)   // K ≈ 3 (oversubscribe)
part → 1 chunk    if whole_part OR part.getMarksCount() ≤ MIN_MARKS_PER_TASK
       else         ceil(part_marks / chunk_marks) contiguous chunks
num_threads  = min(max_threads_for_indexes_or_num_streams, total_work_items)   // no longer capped by part count
```

Global mark budget with a min-chunk floor: targets `num_threads * K` work items
across all parts combined, so load is balanced regardless of how marks are
distributed across parts, while small parts (and trivial analyses) are never
over-split.

New settings:

- `merge_tree_min_marks_per_index_analysis_task` (UInt64): the `MIN_MARKS_PER_TASK`
  floor. Default chosen by benchmarking (see Open items); the floor makes the
  feature a no-op for small parts.
- A kill-switch toggle (UInt64/Bool, default on). The floor already neutralizes the
  feature for small inputs; the toggle is an explicit escape hatch.

`K` is a constant (≈3) unless benchmarking argues otherwise.

### `whole_part` fallback

`whole_part = true` when the part's query uses any of:

- exact ranges requested (`find_exact_ranges`) — the precision guard (see "Caveat"
  under per-chunk processing); looser PK boundaries are unacceptable here,
- the disjunction bitset path (`use_skip_indexes_for_disjunctions`,
  `partial_eval_results` + `mergePartialResultsForDisjunctions`, `:1007`–`:1069`),
- the top-K min-max optimization (`perform_top_k_optimization`, `:1074`–`:1093`),
- FINAL exact mode (`is_final_query && use_skip_indexes_if_final_exact_mode_`,
  `:982`),
- a vector-similarity index (filtering early-returns unless the input covers the
  whole part).

These carry per-part-global state (a part-wide bitset with word-level bit sharing;
a part-global top-K that needs all granules; a whole-part snapshot). Such a part
becomes a single work item run through the **current `process_part` body,
verbatim**, including its inline stats. This keeps those paths byte-for-byte
identical and confines all new logic to the chunked path. Proper per-chunk support
for them is deferred to a follow-up.

### Per-chunk processing (chunked path)

```
process_chunk(item):
    if abort_flag.load(): return
    pk      = markRangesFromPKRange(part, item.sub_ranges, ...)     // on the sub-range only
    ranges  = run skip-index filtering loop over survivors in pk
    chunk_results[item.part_index][item.chunk_index] = { ranges, exact_ranges, read_hints, stat-deltas }
    // granule-level atomics updated here; part-level counters NOT touched
    accumulate total_rows; if over limit → set abort_flag (+ result.exceeded_row_limits if has_projections)
```

**Correctness of concatenation (key argument).** Both PK algorithms are
range-local: binary search returns the intersection of the predicate's continuous
interval with its input ranges, and generic-exclusion search seeds its stack from
the input ranges and only subdivides within them. Concatenating a part's chunk
results in chunk order therefore reproduces a **superset-or-equal** of the
whole-part selection that contains exactly the same matching rows — so **query
results are identical**. The skip-index filtering pass is granule-disjoint and so
is bit-for-bit identical across chunkings.

**Caveat — PK pruning is marginally looser under chunking (verified empirically).**
`markRangesFromPKRange`'s binary search rounds *outward* at the boundary of each
input range it is handed (it keeps a boundary granule that "may be true"). The
whole-part call has only the two true endpoints of the predicate's interval;
splitting a part into N sub-ranges introduces up to N artificial boundaries, each of
which can keep one extra edge granule. So the selected marks are **not** bit-for-bit
identical — chunking selects a bounded number of extra granules (`O(chunk count)`),
which are then filtered out while reading. Measured: a continuous PK range over a
~3900-mark part selected 2345 granules whole-part vs 2347 with ~12 chunks and 2364
with ~490 chunks; the query `count` was identical in every case. At production chunk
sizes (floor in the low thousands of marks → a handful of chunks per part) the
excess is negligible; it is only visible under deliberate stress (`min_marks=8`).

**Precision guard (fail-close).** When the caller requests exact ranges
(`find_exact_ranges`), looser PK boundaries are unacceptable, so such a part is
forced onto the whole-part path (added to the `whole_part` predicate alongside
disjunctions / top-K / FINAL-exact / vector indexes). When a precise result is
needed, the optimization is disabled rather than approximated.

### Stats and limits

- **Granule-level** atomics (`pk_stat`/`useful_indices_stat`: `total_granules`,
  `granules_dropped`, `elapsed_us`) are accumulated per chunk; disjoint granules
  sum correctly. `elapsed_us` becomes a sum over more, smaller tasks — acceptable
  (diagnostic only).
- **Part-level** counters (`total_parts`, `parts_dropped`, `search_algorithm`,
  `skip_index_used_in_part`) cannot be decided per chunk (a part is "dropped" only
  if *all* its chunks drop everything) and are deferred to the post-pass.
- **Row-limit fail-fast** is preserved: the existing `total_rows` atomic
  (`:942`, `:1100`) is checked inside each chunk; on exceed, an atomic `abort_flag`
  is set so other in-flight items short-circuit at their next check. `has_projections`
  still sets `result.exceeded_row_limits` and returns gracefully.

### Post-pass (sequential, after `pool.wait()`)

For each **chunked** part:

1. Concatenate `chunk_results[part_index]` in chunk order into `ranges.ranges`,
   `ranges.exact_ranges`, `ranges.read_hints`. Contiguous, ordered chunks preserve
   global mark order.
2. Part-level stats: `total_parts += 1`; `parts_dropped += merged.empty()`;
   `skip_index_used_in_part[part_index] = 1` if any chunk used a skip index.

Whole-part items already wrote their final ranges and stats inline; the post-pass
skips them.

### Scheduler (design 2)

Materialize all work items (mix of `whole_part` and chunk items) into a flat
vector and `scheduleOrThrow` each into one shared `ThreadPool` sized `num_threads`.
The shared queue load-balances: a thread finishing a small item pulls the next from
the queue. The `num_threads <= 1` sequential branch is preserved.

**Design 3 (later):** keep the same `WorkItem` model and post-pass; replace the
static schedule with an adaptive queue where a thread that finds the queue empty
splits the largest still-running chunk further, eliminating the tail where one
oversized chunk outlives all others.

### Thread-safety

- `mark_cache` / `uncompressed_cache` / `vector_similarity_index_cache` are already
  used concurrently across parts today.
- Concurrent chunks of the *same* part only read a const `data_part`;
  `filterMarksUsingIndex` constructs its own granule readers per call.
- Disjoint sub-ranges → disjoint granules → no shared mutable per-granule state on
  the chunked path. The one structure with word-level bit sharing (the disjunction
  bitset) is excluded via `whole_part`.
- `getAlterConversionsForPart` (`:990`) is read-only and can be hoisted to
  once-per-part to avoid recomputation per chunk.

## Testing

- **Equivalence** (new dedicated `0_stateless` test): a matrix — PK binary search /
  generic-exclusion; 0 / 1 / N skip indexes; FINAL; disjunctions; top-K; sampling;
  parallel-replicas mark allocation; single huge part; many tiny parts; empty
  result — asserting identical selected marks/granules (`system.query_log`
  ProfileEvents) and identical query results with the toggle on vs off.
- **Concurrency stress**: force `merge_tree_min_marks_per_index_analysis_task = 1`
  to maximize chunking and run under TSan and ASan.
- **Perf**: the motivating few-large-parts query (expect speedup) plus the standard
  perf suite to confirm no regression on the many-tiny-parts case (task overhead).
  Run on the metal box.

## Open items (resolve during planning)

- `read_hints` merge semantics across chunks: confirm how `filterMarksUsingIndex`
  returns `ranges.read_hints` and that per-chunk hints concatenate correctly.
- Default values for `MIN_MARKS_PER_TASK` and `K`.
