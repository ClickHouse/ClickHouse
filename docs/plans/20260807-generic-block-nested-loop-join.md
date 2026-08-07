# Generic block nested loop join as the last-resort fallback

## Overview

Implement a block nested loop (BNL) join operator that evaluates an arbitrary `JOIN ON` condition
inside the operator, and route to it every keyless non-equi join that currently throws
`INVALID_JOIN_ON_EXPRESSION`. After this change no `JOIN ON` expression is rejected for the reason
"cannot determine join keys": the planner always has a correct, memory-bounded operator to fall back
to.

The operator is built on the analyzer infrastructure only. It does **not** implement `IJoin` and
never constructs a `TableJoin`. `JoinKind`/`JoinStrictness` come from `JoinOperator`, the predicate
stays an `ActionsDAG` sub-DAG extracted from `JoinExpressionActions`, and the physical step is
constructed directly in `buildPhysicalJoinImpl`.

### Problem it solves

`buildPhysicalJoinImpl` (`src/Processors/QueryPlan/JoinStepLogical.cpp:1392-1421`) handles a `JOIN ON`
with no cross-side equality in three ways:

1. IEJoin claims it, if it has two inequalities and a supported kind/strictness.
2. `tryAddDisjunctiveConditions` claims it, if it is a disjunction of equi-clauses.
3. Otherwise `can_convert_to_cross` — true only for `INNER`/`CROSS` with `ALL` strictness and `hash`
   enabled — rewrites it to `CROSS JOIN` plus a post-join `FilterStep`; if false, it throws.

So `LEFT JOIN t2 ON t1.a < t2.b`, `SEMI JOIN t2 ON f(t1.x, t2.y)`, and every `ANY`/`ANTI` non-equi
join are unsupported. Case 3's success path is also weak: `ConstantJoin` materializes the entire
cartesian product and the predicate is evaluated above the join.

### Key benefits

- Every kind/strictness combination gets a correct implementation of an arbitrary `ON` condition.
- The predicate is evaluated on `(left_row, right_row)` index pairs inside the operator, so peak
  memory is bounded by one tile plus the build side, not by the cartesian product.
- Early exit for `ANY`/`SEMI`/`ANTI`: stop scanning the build side after the first match per probe row.

### Deliberately out of scope

These are the follow-ups that make a BNL fast rather than merely correct. They are listed here so the
first version is not accidentally scoped to include them, and so they are not forgotten. Each is also
recorded as a `TODO` at the code site where it would land, which is the only place for follow-up notes
this repository has (see the decisions in task 11):

- Per-tile min/max pruning: interval arithmetic over the predicate against each stored block's
  min/max, to skip provably-empty tiles and shortcut provably-all-true ones
  (`BlockNestedLoopJoinTransform.cpp`, `matchNextTile`).
- Choosing the smaller side as the inner side (requires the planner to swap inputs, as
  `IEJoinStep::swap_inputs` does for right-side `SEMI`/`ANTI`) (`JoinStepLogical.cpp`, at the site
  that selects the operator).
- Grace partitioning when the build side does not fit memory (v1 spills sequentially instead)
  (`BlockNestedLoopJoinData.cpp`, `spillInMemoryBlocks`).
- Absorbing the existing `CROSS` + post-filter rewrite and the disjunctive path. Both work today;
  taking them over risks regressions and needs perf validation first (`JoinStepLogical.cpp`, at the
  same site).

## Context (from discovery)

### Files and components involved

- `src/Processors/QueryPlan/JoinStepLogical.cpp` — `buildPhysicalJoinImpl`. Two hooks:
  - the `!has_keys` branch (lines 1392-1421), where the decision to use BNL is made instead of
    throwing;
  - the `on_clause_condition` handling (lines 1546-1558), where the condition is turned into an
    `ExpressionActionsPtr` for the operator. For BNL the whole `ON` condition must go into the
    operator **regardless of `canPushDownFromOn`** — for `INNER` the current code would otherwise
    turn it into a post-join filter, which is exactly the cartesian-product materialization being
    avoided.
- `src/Processors/QueryPlan/IEJoinStep.{h,cpp}` (212 + 70 lines) — the structural template for a
  `TableJoin`-free physical join step: `updatePipeline`, `updateOutputHeader` returning
  `concatHeaders(input_headers)`, `describeActions`, and a static `isSupportedJoinType`.
- `src/Processors/Transforms/IEJoinTransform.{h,cpp}` — `IEJoinResidualCondition` (an
  `ExpressionActionsPtr` plus, per required column, a `{side, position}` `Source`) is the
  representation to copy for the BNL predicate. `IEJoinTransform.cpp:1135-1138` is the reference for
  NULL handling, and `:1155` documents that padded columns are filled with the column's default.
- `src/QueryPipeline/QueryPipelineBuilder.cpp:466-620` — `joinPipelinesRightLeft`, the build-then-probe
  pipeline shape BNL needs, but keyed on `JoinPtr`. Note that `IEJoinStep` uses `joinPipelinesPaired`
  instead, which is a *single* processor with two inputs — correct for IEJoin, wrong for BNL.
- `src/Processors/Transforms/JoiningTransform.h:95-117` — `FillingRightJoinSideTransform`, the model
  for the build-side processor, including its `getMemoryStats`/`spillOnSize` overrides.
- `src/Interpreters/HashJoin/ScatteredBlock.h:376-394` — `StoredBlock` (columns + `Selector` +
  `replicated_columns` fast path), reusable as the build-side block representation.
- `src/Columns/ColumnReplicated.h` — nested column + index column. This is how a tile is built without
  copying: the probe-side tile column is `ColumnReplicated(probe_block_column, repeated_indexes)`.
- `src/Interpreters/ConstantJoin.cpp` (914 lines) — the current cross-product engine. Its spill and
  compression code (`trySpillRightBlock`, `storeRightBlock`, `shrinkStoredBlocksToFit`) is the prior
  art for the build-side store, but the class itself is not reused: its documented invariant is that
  "the whole strategy is fixed at construction time" because the predicate is constant.

### Related patterns found

- **`addToNullableIfNeeded` (`JoinStepLogical.cpp:175`) already made the padded side's columns
  `Nullable` in the pre-join actions.** Consequence: the operator emits an unmatched row by inserting
  each padded column's *default*, and there is no need for a `NotJoinedBlocks` equivalent (which is
  fortunate, since `NotJoinedBlocks` takes a `const TableJoin &`). This is what makes the
  `TableJoin`-free route genuinely simpler rather than merely different.
- `IEJoinStep` handles right-side `SEMI`/`ANTI` by swapping the input pipelines and appending a
  `ColumnPermuteTransform` to restore column order. BNL does not need this in v1 (it walks both sides
  explicitly), but it is the precedent if the "smaller side as inner" follow-up is taken.
- `gtest_full_sorting_join.cpp` in `src/Processors/tests/` is the precedent for unit-testing a join
  algorithm's core outside the pipeline.

### Dependencies identified

- **Optimizer passes keyed on `JoinStep` + `getTableJoin()` will not see the new step.** Each must be
  confirmed to skip rather than to conclude "no join is present": `filterPushDown.cpp:493`,
  `optimizeReadInOrder.cpp:154,1208`, `optimizeJoin.cpp:528`, `topKThroughJoin.cpp:41`,
  `convertOuterJoinToInnerJoin.cpp:31`, `optimizeJoinByShards.cpp:388,543`,
  `calculateHashTableCacheKeys.cpp:246`, `considerEnablingParallelReplicas.cpp:197`,
  `ParallelReplicasLocalPlan.cpp:55,102`. `removeRedundantSorting.cpp:287` already had to be taught
  about `IEJoinStep` explicitly and is the example of a pass that needs a real change.
- **No settings plumbing is required.** Selection is structural (no equi key, no IEJoin, not
  disjunctive), so it is deterministic on replicas. `JoinStepLogical::deserialize` rebuilds settings
  from `QueryPlanSerializationSettings` and runs `buildPhysicalJoin` on each replica; because nothing
  in the decision depends on a new setting, no `QueryPlanSerializationSettings` entry is needed.
- Physical join steps are not serialized (only `JoinStepLogical` is), so no
  `QueryPlanStepRegistry` registration is needed either.

## Development Approach

- **Testing approach**: regular (code first, then tests). ClickHouse join behaviour is verified by
  stateless SQL tests under `tests/queries/0_stateless/`; TDD is not workable here because most tasks
  need a built server to produce any observable behaviour. A gtest is used for the tile-matching core,
  which is the one piece that is testable in isolation.
- Complete each task fully before moving to the next.
- Make small, focused changes.
- Update this plan file when scope changes during implementation
- Run added tests after each change
- Maintain backward compatibility: queries that work today must keep the same plan and results. The
  only intended behaviour change is that queries which previously threw `INVALID_JOIN_ON_EXPRESSION`
  now return results.

## Testing Strategy

- **SQL tests**: `tests/queries/0_stateless/`, created with `./tests/queries/0_stateless/add-test <name>`.
- Build with the `build` skill; run tests via `./tests/clickhouse-test -b ./path/to/clickhouse test_name` redirecting output to `build/test_<name>.log` and analyse
  the log with the `build-log` subagent rather than reading it in the main context.

## Progress Tracking

- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix
- Update plan if implementation deviates from original scope
- Keep plan in sync with actual work done

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): code, tests, docs inside this repository.
- **Post-Completion** (no checkboxes): pushing the branch, opening the PR, watching CI, perf runs.

## Implementation Steps

### Task 1: Route keyless non-equi joins to a new step instead of throwing

- [x] create branch `vdimir/block-nested-loop-join` from `master` (done as
      `generic-block-nested-loop-join`, cut from `master`)
- [x] add `src/Processors/QueryPlan/BlockNestedLoopJoinStep.{h,cpp}`: constructor taking left/right
      headers, the predicate (`ExpressionActionsPtr` + per-required-column `{side, position}` sources,
      modelled on `IEJoinResidualCondition`), `JoinKind`, `JoinStrictness`, `SizeLimits`, and the
      output block size limits; `updateOutputHeader` returning `concatHeaders(input_headers)`;
      `updatePipeline` throwing `NOT_IMPLEMENTED` for now
- [x] add a static `BlockNestedLoopJoinStep::isSupportedJoinType(kind, strictness)` as the single
      source of truth for the supported matrix (everything except `ASOF` and `PASTE`)
- [x] in `buildPhysicalJoinImpl`, in the `!ie_join_description && !has_keys` branch
      (`JoinStepLogical.cpp:1392-1421`): when `tryAddDisjunctiveConditions` returns false and
      `can_convert_to_cross` is false, select BNL instead of throwing `INVALID_JOIN_ON_EXPRESSION`;
      keep the throw only for kind/strictness combinations `isSupportedJoinType` rejects
- [x] extend the `on_clause_condition` handling (`JoinStepLogical.cpp:1546-1558`) so that when BNL is
      selected the condition is always extracted into the operator's `ExpressionActionsPtr`, bypassing
      the `canPushDownFromOn` check, and add a `constructBlockNestedLoopJoinStep` helper mirroring
      `constructIEJoinStep` (pre-join actions on each input, post-join actions and residual filter on
      top)
- [x] write `EXPLAIN` tests asserting the new step appears for `LEFT`/`RIGHT`/`FULL`/`SEMI`/`ANTI`
      non-equi joins, and that `INNER`/`CROSS` non-equi joins still produce the existing
      `CROSS JOIN` + filter plan (no takeover)
      (`tests/queries/0_stateless/04813_block_nested_loop_join_routing.sql`)
- [x] write tests for the error cases: `ASOF` with a non-equi-only `ON` still throws
      `INVALID_JOIN_ON_EXPRESSION`
- [x] run tests - must pass before task 2

**Decisions recorded in task 1**

- The single-side condition extraction (`analyzer_left/right_filter_condition_column_name`) is
  skipped for BNL as it is for IEJoin: with no equi key there is no `TableJoin` clause to attach it
  to, and the operator evaluates the whole `ON` condition anyway.
- `tryAddDisjunctiveConditions` is called with `throw_on_error = false` whenever BNL can take the
  join, so a disjunction with a keyless disjunct (`ON a = b OR c < d` for an outer kind) routes to
  BNL instead of throwing. A disjunction whose every disjunct has keys is still claimed as hash
  clauses, unchanged.
- ⚠️ Existing tests that asserted `INVALID_JOIN_ON_EXPRESSION` for a now-routed shape were changed
  to expect `NOT_IMPLEMENTED` (the operator's own error) and carry a comment saying so:
  `00800_low_cardinality_join`, `00800_low_cardinality_merge_join`, `01478_not_equi-join_on`,
  `03362_join_where_false_76670`, `03984_table_function_arg_in_remote_with_join_using`,
  `04259_cte_subquery_dot_column_leak`, `04522_ie_join_duckdb_unsupported_shapes`,
  `04544_ie_join_detection_negatives`, `04556_ie_join_detection_strictness_negatives`,
  `04560_ie_join_tuple_dynamic_keys`. Tasks 4-6 must replace each of those with a result assertion
  as the corresponding kind/strictness starts working.

### Task 2: Build-side store `BlockNestedLoopJoinData`

- [x] add `src/Processors/Transforms/BlockNestedLoopJoinData.{h,cpp}`: a shared, thread-safe store of
      build-side blocks as `StoredBlock` (`ScatteredBlock.h:376`), with total row/byte counters, a
      `SizeLimits` check on insert, and a `finish()` barrier after which the store is read-only
- [x] add `BlockNestedLoopBuildTransform` (an `IProcessor` with one input and no output, modelled on
      `FillingRightJoinSideTransform`) that feeds chunks into the store and signals completion through
      a `FinishCounter`
- [x] handle the empty-build-side case explicitly in the store (`INNER`/`SEMI` produce nothing;
      `LEFT`/`ANTI` produce all probe rows padded)
- ➕ [x] unit-test the store in `src/Processors/tests/gtest_block_nested_loop_join_data.cpp`: row
      offsets, columnless blocks, `Const`/`Sparse` materialization, the read-only barrier, both
      overflow modes, concurrent inserts, and the empty-build-side matrix

**Decisions recorded in task 2**

- `finish` assigns the global row numbering (`getRowOffsets`) rather than maintaining it on insert,
  because concurrent build streams append in an arbitrary order; `StoredBlock::block_no` is the
  insertion index and is unique, so a global row number resolves to exactly one stored row.
- The build transform keeps `FillingRightJoinSideTransform`'s empty-header output port: it carries no
  data, and its only role is to be finished once the store is closed, which is the edge task 3 needs
  to hold the probe side back. Every exit path counts the stream out through the `FinishCounter`
  exactly once, including the one taken when the output is finished from downstream.
- `addBlock` takes the chunk's row count separately from the block, because a build side pruned to
  zero columns (`SELECT count() FROM t1 LEFT JOIN t2 ON t1.a > 5`) still has rows, and an empty build
  side is not the same as a build side of rows with nothing selected from it.
- Stored columns are materialized out of `Const` and `Sparse` but keep `ColumnReplicated`, which
  `StoredBlock` supports natively and which unwrapping would copy.

### Task 3: Pipeline wiring in `BlockNestedLoopJoinStep::updatePipeline`

- [x] wire the build-then-probe graph locally in `updatePipeline` using `QueryPipelineBuilder`
      primitives (`resize`, `transform`, `addTransform`) rather than adding a `JoinPtr`-free variant of
      `joinPipelinesRightLeft`: resize the build pipeline to `max_streams`, attach one
      `BlockNestedLoopBuildTransform` per stream sharing one `FinishCounter`, then resize to 1 and
      connect it as a delayed dependency of the probe pipeline, which keeps `max_streams` probe streams
- [x] decide and implement `WITH TOTALS` behaviour: join the probe-side totals row against the build
      side the same way `JoinCommon::joinTotals` does, or reject `WITH TOTALS` on this path with a
      clear message if the semantics cannot be matched (record the decision in this plan)
- [x] implement `describePipeline` and `describeActions` (both `FormatSettings` and `JSONBuilder`
      overloads) showing kind, strictness and the predicate
- [x] write SQL tests for `EXPLAIN PIPELINE` showing the build and probe processors
      (`tests/queries/0_stateless/04814_block_nested_loop_join_pipeline.sql`)
- [x] run tests - must pass before task 4 (⚠️ end-to-end `SELECT` still fails until task 4 lands;
      mark any test that depends on it accordingly)

**Decisions recorded in task 3**

- **`WITH TOTALS` is supported, with `JoinCommon::joinTotals`' semantics.** That function does not
  match anything: it extends the left totals row with the right totals row, filling with defaults
  where a side has no totals of its own. `BlockNestedLoopTotalsTransform` reproduces it by position
  rather than by name, and takes the nullability of the pre-join actions as given, so no `TableJoin`
  is needed. When the probe side has no totals but the build side does, one is synthesized with
  `addDefaultTotals` and dropped again if the build side turns out to deliver none, mirroring
  `JoiningTransform`'s `default_totals`. A build side with totals uses a single build stream, the one
  that owns the totals port; without them the build fans out to `max_streams`.
- The build-then-probe edge is `QueryPipelineBuilder::addPipelineBefore`, the existing "run this
  pipeline to completion first" primitive: the build transforms' empty-header output ports become the
  non-delayed inputs of a `DelayedPortsProcessor` whose delayed ports are the probe streams *and* the
  probe totals stream. No `JoinPtr`-free variant of `joinPipelinesRightLeft` was added.
- ➕ `QueryPipelineBuilder::dropExtremes` was added: a join must drop its inputs' extremes while
  keeping their totals, and only the combined `dropTotalsAndExtremes` was public.
- `BlockNestedLoopPredicate` moved from the step header to
  `src/Processors/Transforms/BlockNestedLoopJoinTransform.h`, where `BlockNestedLoopProbeTransform`
  lives, mirroring how `IEJoinResidualCondition` sits in `IEJoinTransform.h`.
- ➕ A probe chunk with no rows produces no output. This is decided by the probe transform now rather
  than in task 4, because an empty probe side otherwise depends on whether the source pushes a
  zero-row chunk at all, which is not deterministic.
- The step reports `build` and `probe` stage groups (`getStepGroups`), so `EXPLAIN ANALYZE` attributes
  materializing the right input and matching separately.

### Task 4: Probe core - tile evaluation, `INNER` and `LEFT` with `ALL` strictness

- [x] add `BlockNestedLoopProbeTransform` (`IProcessor`, one input one output) that, for each probe
      chunk, walks the stored build blocks and produces output chunks
- [x] implement tile construction: for a tile of `probe_rows x build_rows`, build each side's columns
      as `ColumnReplicated` over the source column plus an index column, so no source data is copied
- [x] implement predicate evaluation over the tile using the step's `ExpressionActions`, assembling
      its input block from the `{side, position}` sources; treat a `Nullable` result's NULL as
      not-matched (as `IEJoinTransform.cpp:1135-1138` does)
- [x] implement pair enumeration into `(probe_index, build_index)` vectors and materialise output
      columns only once per output chunk, respecting `max_joined_block_size_rows` /
      `max_joined_block_size_bytes`
- [x] implement resumable state (probe row cursor, build block cursor, offset within block) so a
      single probe chunk can span many output chunks without materialising them all
- [x] implement `LEFT ALL`: track per-probe-row match so an unmatched probe row is emitted once with
      the build-side columns set to their defaults
- [x] write tests for the matching core: tile pair enumeration, NULL-as-false, output chunking at a
      size limit, resumption across `work()` calls
      (`src/Processors/tests/gtest_block_nested_loop_join_probe.cpp`)
- [x] write an SQL test comparing `INNER`/`LEFT` non-equi results against the equivalent
      `CROSS JOIN` + `WHERE` rewrite, over a table with NULLs, on several predicate shapes
      (`<`, `BETWEEN`, `a + b > c`, a non-monotone function, a predicate that is always false)
      (`tests/queries/0_stateless/04815_block_nested_loop_join_inner_left.sql.j2`)
- [x] write an SQL test for `join_use_nulls = 0` and `= 1`
      (`tests/queries/0_stateless/04816_block_nested_loop_join_use_nulls.sql.j2`)
- [x] run tests - must pass before task 5

**Decisions recorded in task 4**

- The tile is sized in pairs (`DEFAULT_BLOCK_SIZE`) independently of the output limit: it bounds the
  intermediate columns the condition is evaluated on, while the output chunk is cut to
  `max_block_size` when the accumulated pairs are materialized. A build block is walked in
  sub-ranges of `max(1, DEFAULT_BLOCK_SIZE / probe_rows)` rows, so the tile never grows with the
  build block.
- The tile columns are `ColumnReplicated` only where `isLazyReplicationUseful` says so, mirroring
  `replicateColumnsLazily`; for narrow fixed-size values a gather costs less than the indirection.
  The two sides do not share an index column, so a function over both materializes the tile - which
  is the intended bound, one tile rather than the cartesian product.
- The condition column is turned into a match mask by `FilterDescription`, which is where
  "NULL is not a match" comes from, together with the handling of a `Nullable`, `LowCardinality`,
  `Const`, `Sparse` or non-`UInt8` numeric condition. `ConstantFilterDescription` short-cuts a
  condition that folded to a constant.
- An output chunk may span several stored build blocks; the pairs are accumulated as
  `(probe row, row in block)` plus a run per block, and each run is gathered from its own block.
  A single run - the common case - is gathered without a concatenation.
- `work` yields with no output every `8 * max(DEFAULT_BLOCK_SIZE, max_block_size)` evaluated pairs,
  so a walk over a build side that matches nothing stays cancellable.
- ⚠️ **The swap of the join inputs (`query_plan_join_swap_table`) turns a `LEFT` join into a `RIGHT`
  one**, which this task does not implement, so every test that executes a keyless `LEFT` join pins
  the setting off. Tasks 5 and 6 must remove those pins as the swapped kinds start working:
  `01478_not_equi-join_on`, `03362_join_where_false_76670`,
  `03984_table_function_arg_in_remote_with_join_using`, `04259_cte_subquery_dot_column_leak`,
  `04544_ie_join_detection_negatives`, `04560_ie_join_tuple_dynamic_keys`,
  `04815_block_nested_loop_join_inner_left`, `04816_block_nested_loop_join_use_nulls`.
- Tests whose keyless `LEFT`/`INNER` shape now returns a result instead of `NOT_IMPLEMENTED` were
  converted to result assertions: `01478_not_equi-join_on`, `01881_join_on_conditions_merge`,
  `01881_join_on_conditions_merge_decimal`,
  `03984_table_function_arg_in_remote_with_join_using`, `04259_cte_subquery_dot_column_leak`,
  `04544_ie_join_detection_negatives`, `04560_ie_join_tuple_dynamic_keys`,
  `04814_block_nested_loop_join_pipeline`. The remaining `NOT_IMPLEMENTED` expectations are all
  `RIGHT`, `FULL`, `ANY`, `SEMI` or `ANTI`, and belong to tasks 5 and 6.
- The whole `join`-matching stateless suite (1002 tests) was run: everything passes except tests that
  need a setup this machine does not have (the `pr_plan_based_join_*` and
  `04201_max_bytes_ratio_before_external_join_distributed` group needs `process_query_plan_packet`
  in the server config, `03325_sqlite_join_wrong_answer` needs `sqlite3`, and the `hits`/`visits`
  tests need the stateful datasets).

### Task 5: `RIGHT` and `FULL` - build-side used flags and the unmatched emit stage

- [x] add per-build-row match flags to `BlockNestedLoopJoinData` (an `atomic_bool` per row or an
      atomically-updated bitmap), written by every probe stream and read only after all probe streams
      finish; document the memory ordering that makes this safe
- [x] set flags from the probe core when the kind requires them, and skip the bookkeeping entirely for
      kinds that do not (`INNER`, `LEFT`, and the left-driven `SEMI`/`ANTI`)
- [x] add the unmatched-build-rows emit stage as a processor that runs after the probe streams finish
      and emits stored rows whose flag is unset, padded with probe-side column defaults; partition the
      work across streams so it can run in parallel
- [x] handle the empty-probe-side case: `RIGHT`/`FULL` must still emit every build row
- [x] write tests for the flag bitmap: concurrent sets, the disjoint partitioning of the unmatched
      scan across streams
- [x] write an SQL test comparing `RIGHT ALL` and `FULL ALL` non-equi results against a
      `UNION ALL` of the `LEFT` result and an anti-join rewrite
      (`tests/queries/0_stateless/04817_block_nested_loop_join_right_full.sql.j2`)
- [x] write an SQL test for an empty probe side and an empty build side against every kind
      (`tests/queries/0_stateless/04818_block_nested_loop_join_empty_sides.sql.j2`)
- [x] run tests - must pass before task 6

**Decisions recorded in task 5**

- The flags are **one `std::atomic_bool` per build row**, not a packed bitmap: only the rows that
  matched are written, so a relaxed store beats an atomic `fetch_or` on a word shared by 64 rows, and
  a byte per row is negligible next to the materialized build side it indexes. The array is allocated
  by `finish`, which is where the row count is first known.
- **Memory ordering: relaxed for both the writes and the reads.** A flag is only ever set, never
  cleared, so no write can be lost, and the happens-before edge comes from the pipeline rather than
  from these accesses - a probe stream finishes its output port only after its last write, and
  `DelayedPortsProcessor` observes that finish before it lets the unmatched stage produce a row.
  `finished` orders the allocation itself the same way it orders `blocks` and `row_offsets`.
- A build row is named by `getRowOffsets()[block_index]` plus its index into the block's columns. The
  store never scatters a block, so a row's index into its columns is also its position in the block;
  this is what lets the probe turn a tile index straight into a global row number.
- The emit stage is `BlockNestedLoopUnmatchedBuildRowsTransform`, one `ISource` per stream wired as
  the delayed ports of a second `DelayedPortsProcessor` whose main ports are the probe streams - the
  shape `joinPipelinesRightLeft` uses for `NonJoinedBlocksTransform`. The stored blocks are dealt out
  round-robin (`stream_index`, `+ num_streams`, ...), so the streams cover the build side exactly once
  with no shared cursor, and one output chunk comes from one stored block so its build columns are
  gathered in one go.
- The empty-probe-side case needs no code of its own: the probe streams still finish, the delayed
  ports open, and every flag is still unset, so the scan emits the whole build side.
- `needsBuildSideMatchFlags` and `keepsUnmatchedBuildRows` state the whole matrix in one place,
  including the right-driven `SEMI`/`ANTI` that task 6 implements, so there is a single source of
  truth for which kinds keep flags and which emit build rows afterwards.
- Tests whose keyless `RIGHT`/`FULL` shape now returns a result instead of `NOT_IMPLEMENTED` were
  converted to result assertions: `01478_not_equi-join_on`, `01881_join_on_conditions_merge`,
  `01881_join_on_conditions_merge_decimal`, `03362_join_where_false_76670`. The
  `query_plan_join_swap_table` pins task 4 added were removed from `01478_not_equi-join_on`,
  `03362_join_where_false_76670`, `03984_table_function_arg_in_remote_with_join_using`,
  `04259_cte_subquery_dot_column_leak`, `04544_ie_join_detection_negatives`,
  `04560_ie_join_tuple_dynamic_keys`, `04815_block_nested_loop_join_inner_left` and
  `04816_block_nested_loop_join_use_nulls`, and each of those was re-run with the setting pinned both
  ways. `04814_block_nested_loop_join_pipeline` keeps its pin, because it counts pipeline processors
  and the swap changes which input is built. Every remaining `NOT_IMPLEMENTED` expectation is `ANY`,
  `SEMI` or `ANTI` and belongs to task 6.
- The `join`-matching stateless suite (1004 tests) was re-run: 981 pass and the 23 failures are all
  explained by what this machine lacks - `process_query_plan_packet` in the server config
  (`pr_plan_based_join_*`, `04201_max_bytes_ratio_before_external_join_distributed`), `sqlite3`
  (`03325_sqlite_join_wrong_answer`), the `hits`/`visits` stateful datasets, and a local MinIO
  (`03800_s3_cluster_join`).

### Task 6: `SEMI`, `ANTI`, `ANY` strictness with early exit

- [x] implement left-driven `SEMI`/`ANTI`/`ANY`: stop scanning build blocks for a probe row as soon as
      the first match is found, and emit the probe row (`SEMI`, `ANY`) or suppress it (`ANTI`)
- [x] implement right-driven `SEMI`/`ANTI` (`RIGHT SEMI`, `RIGHT ANTI`) on top of the used flags from
      task 5, emitting build rows by flag after the probe phase
- [x] honour `join_any_take_last_row` for `ANY`, or reject it explicitly on this path if honouring it
      would require a full scan that defeats the early exit (record the decision in this plan)
      (ignored on this path - see the decisions below)
- [x] write tests asserting early exit actually happens: a counting predicate that records how many
      pairs were evaluated (`EarlyExitStopsTheBuildSideWalkAtTheFirstMatch` in
      `gtest_block_nested_loop_join_probe.cpp`)
- [x] write SQL tests comparing every `SEMI`/`ANTI` variant against the equivalent
      `EXISTS`/`NOT EXISTS` correlated subquery, and `ANY` against `LIMIT 1` semantics
      (`tests/queries/0_stateless/04819_block_nested_loop_join_semi_anti.sql.j2` and
      `04820_block_nested_loop_join_any.sql.j2`)
- [x] run tests - must pass before task 7

**Decisions recorded in task 6**

- The strictness matrix is one enum, `BlockNestedLoopProbeTransform::PairSelection`, stating how many
  of the pairs that satisfy the condition reach the result: every one (`ALL`, and any strictness on
  an explicit cartesian join), one per probe row (left-driven `ANY`/`SEMI`, and the old `ANY` for
  every kind), one per build row (right-driven `ANY`/`SEMI`), one per row of *both* sides
  (`ANY INNER`), or none (`ANTI`, whose result is made of the rows that matched nothing).
- **`ANY INNER` takes each row of either side at most once**, rather than just one row per probe row.
  The planner swaps a join's inputs for `ANY`/`SEMI`/`ANTI` (`isSwapOnlyJoinStrictness`) and flips
  the kind with `reverseJoinKind`, which leaves `INNER` unchanged - so a left-driven `ANY INNER`
  would return one row per *build* row whenever `query_plan_join_swap_table` fired, which was
  reproducible on this branch before the change. Taking each row of both sides once is the only
  reading that survives the swap, and it is what the manual means by "completely disables the
  cartesian product" for `INNER`. Which rows end up paired is not fixed, as for every `ANY` join.
  This is why `needsBuildSideMatchFlags` now covers `ANY INNER` although it emits no build row.
- A build row is given to one probe row by an atomic `exchange` on its match flag
  (`claimBuildRow`), so the right-driven selections need no lock and no second pass. Relaxed
  ordering is enough: the exchange is atomic whatever the ordering, and nothing but the claim
  travels through it.
- **The early exit is off for the kinds that pad the build rows nothing matched** (`RIGHT`/`FULL`
  with the old `ANY`, and `RIGHT ANTI`). A probe row that stopped at its first match would leave the
  build rows it would have matched later unflagged, and the scan after the probe would then emit
  them as unmatched. The old `ANY` flags every build row it matched - including the ones it passed
  over - which is what `ConstantJoin` does for the same shape, so those rows are dropped rather than
  padded.
- `join_any_take_last_row` is ignored, as the setting's own documentation implies ("applies to `Join`
  engine tables and hash-based join algorithms"): with no join key there is no group of rows to take
  the last of, the store's block order is whatever order the build streams filled it in, and
  honouring it would cost the early exit that makes `ANY` worth choosing. `FullSortingMergeJoin` is
  the precedent - `PlannerJoins` passes the setting only to the hash-based algorithms.
- The SQL tests assert what an `ANY` join guarantees rather than a fixed result: every emitted pair
  satisfies the condition, no id of the driving side repeats, and a row is padded exactly when
  nothing matches it. `ANY INNER` is checked with `query_plan_join_swap_table` pinned both ways.
- Tests whose keyless `ANY`/`SEMI`/`ANTI` shape now returns a result instead of `NOT_IMPLEMENTED`
  were converted to result assertions: `00800_low_cardinality_join`,
  `00800_low_cardinality_merge_join`, `04522_ie_join_duckdb_unsupported_shapes`,
  `04544_ie_join_detection_negatives`, `04556_ie_join_detection_strictness_negatives`,
  `04560_ie_join_tuple_dynamic_keys`. No `NOT_IMPLEMENTED` expectation from this feature is left:
  the probe now covers everything `BlockNestedLoopJoinStep::isSupportedJoinType` admits, and
  `isImplementedByProbe` is gone.

### Task 7: Memory bounding and spilling of the build side

- [x] implement `getMemoryStats` and `spillOnSize` on `BlockNestedLoopBuildTransform` so the query
      memory tracker can ask the build side to shrink, mirroring `FillingRightJoinSideTransform`
- [x] implement block compression and sequential spill-to-disk in `BlockNestedLoopJoinData` when
      `max_bytes_before_external_join` / the memory-pressure callback demands it, using
      `TemporaryDataOnDiskScope`; the probe then re-reads spilled blocks per probe chunk
      (⚠️ the spill trigger is `max_bytes_before_external_join`, not `max_bytes_in_join` - see the
      decisions below)
- [x] make the used-flags indexing correct across spilled blocks (flags are indexed by global build
      row number, which must stay stable when a block moves to disk)
- [x] write tests for the store with a forced spill threshold: round-trip of stored rows, flag
      indexing across the in-memory/spilled boundary
      (`gtest_block_nested_loop_join_data.cpp`, and the probe against a spilled and a compressed
      build side in `gtest_block_nested_loop_join_probe.cpp`)
- [x] write an SQL test with `max_bytes_before_external_join` set low enough to force a spill,
      asserting the result equals the unspilled result, plus a test for
      `join_overflow_mode = 'throw'` and `= 'break'`
      (`tests/queries/0_stateless/04821_block_nested_loop_join_spill.sql.j2` and
      `04822_block_nested_loop_join_overflow.sql`)
- [x] run tests - must pass before task 8

**Decisions recorded in task 7**

- **The spill trigger and the size limits are separate knobs.** `max_rows_in_join` /
  `max_bytes_in_join` stay a hard limit on the build side as a whole, spilled blocks included, so
  `join_overflow_mode = 'throw'` throws and `= 'break'` breaks at the same point whether the build
  side fits in memory or not. Spilling is driven by `max_bytes_before_external_join` /
  `max_bytes_ratio_before_external_join` - the knob the hash join spills on - and by the query
  memory tracker through `spillOnSize`. `ConstantJoin` conflates the two (it spills *because* the
  size limits are near, and then does not check them), which makes `join_overflow_mode`
  unobservable as soon as a temporary volume is configured; that is not worth copying.
- Compression uses the cross join's thresholds (`cross_join_min_rows_to_compress`,
  `cross_join_min_bytes_to_compress`), because a block nested loop join is a cross join with a
  predicate on top and keeps its build side for exactly the same reason.
- **The spill file is written once, in block-index order, and read forward only.**
  `TemporaryBlockStreamHolder` has no seek, so random access would mean one file per block. Instead
  every consumer of the store walks the blocks by increasing index - the probe over its chunk, the
  unmatched scan over its share - and a `BuildSideBlockReader` turns that into one forward pass,
  starting the file over when it is asked for an earlier block (which is what a new probe chunk
  does). The order invariant holds because a block's index and its position in the file are both
  assigned under the store's mutex.
- The cost this leaves on the table, deliberately: each probe stream reads the whole spilled file
  once per probe chunk, and each unmatched-scan stream reads it once in full even though it only
  emits every `num_streams`-th block. Per-tile pruning and grace partitioning - the fixes - are in
  "Deliberately out of scope".
- A `BuildRun` keeps a `shared_ptr` to the block its pairs came from, so an output chunk can still
  be materialized from a block the walk has passed - which a compressed or spilled block would
  otherwise have to be read back a second time for. Only the runs with pairs still pending hold one.
- **A build side of columnless rows never spills**: it is a row count rather than data, the `Native`
  format cannot persist it, and it costs no memory. `canSpill` states this once, and it is also what
  decides whether the build transform reports itself `spillable` to the memory tracker at all.
- `need_reserved_memory_bytes` is twice the largest block held in memory, not a multiple of the
  whole store as `GraceHashJoin` reports: the spill writes the blocks out one at a time, so the
  memory it needs on top of what it frees is one block decompressed plus the buffer it is
  compressed into.
- The store's `getBlocks()` is gone. A block is now named by its index, and `getNumBlocks` /
  `getBlockNumRows` answer without reading it back - which is what lets the unmatched scan decide
  from the flags alone that a block contributes nothing and skip it.

### Task 8: Audit the query plan optimizer passes

- [x] for each pass listed in Context, confirm by reading it that an unrecognised join step causes it
      to skip rather than to assume no join is present: `filterPushDown.cpp:493`,
      `optimizeReadInOrder.cpp:154,1208`, `optimizeJoin.cpp:528`, `topKThroughJoin.cpp:41`,
      `convertOuterJoinToInnerJoin.cpp:31`, `optimizeJoinByShards.cpp:388,543`,
      `calculateHashTableCacheKeys.cpp:246`, `considerEnablingParallelReplicas.cpp:197`,
      `ParallelReplicasLocalPlan.cpp:55,102`
- [x] fix any pass that does not degrade safely, and teach `removeRedundantSorting.cpp:287` about the
      new step alongside `IEJoinStep` if a sort above it could be wrongly removed
      (no pass needed a change - see the decisions below)
- [x] write an SQL test with a `WHERE` above a BNL join, asserting filter pushdown either happens
      correctly or is safely skipped
      (`tests/queries/0_stateless/04823_block_nested_loop_join_optimizer_passes.sql`)
- [x] run tests - must pass before task 9

**Decisions recorded in task 8**

- **No optimizer pass needed a change.** Every pass that keys on a physical join skips the operator,
  and every pass that would be *wrong* to skip runs before physicalization, on the `JoinStepLogical`
  the operator is built from. The two hooks in `optimizeTree.cpp` that matter are
  `convertLogicalJoinToPhysical` (called on leave of the second-pass traversal) and the first pass,
  which runs entirely on logical joins.
- `filterPushDown.cpp:467` - the step is neither `JoinStep`, `FilledJoinStep` nor `JoinStepLogical`,
  so `tryPushDownOverJoinStep` returns 0. Nothing is lost: the `WHERE` is pushed while the join is
  still logical, so it reaches the input it names, down to that input's `PREWHERE`. The pass is
  re-run after physicalization only when a join runtime filter was added, and there it correctly
  declines.
- `optimizeReadInOrder.cpp:147` - `findReadingStep` recognizes only `JoinStep`/`FilledJoinStep`, so
  read-in-order is not propagated through the operator and every caller returns before
  `buildSortingDAG` - the one function that would otherwise walk through the step's first child as
  if it were transforming. `buildSortingDAG`'s two other callers cannot reach the step either:
  `wouldReadInOrderBeUseful` is called from `topKThroughJoin` only after the step was recognized as
  a join, and from `optimizeUseNormalProjection` on a subtree `QueryDAG::build` has already rejected
  for containing a step with two children.
- `topKThroughJoin.cpp:284` - `getJoinSemanticsFromStep` returns `nullopt` and the pass returns 0. In
  practice it fires earlier, on the `JoinStepLogical`, and its soundness argument (every
  preserved-side row yields at least one output row) holds for the operator as it does for a hash
  join.
- `convertOuterJoinToInnerJoin.cpp:27` - the legacy path requires a `JoinStep`; the analyzer path at
  `:222` works on the `JoinStepLogical`, so an outer keyless join is still narrowed by a `WHERE` on
  its padded side. `FULL` narrows to `LEFT` and keeps the operator, `LEFT` narrows to `INNER` and
  goes back to the cross join with a filter - both pinned by the new test.
- `optimizeJoin.cpp:518` (`optimizeJoinLegacy`), `optimizeJoinByShards.cpp:376,539` and
  `optimizeJoinLazyIndexing.cpp:24` all require a `JoinStep`. The DFS in `optimizeJoinByShards` also
  drops its accumulated per-branch state at any two-child step it does not recognize, so no sharding
  decision travels across the operator.
- `calculateHashTableCacheKeys.cpp:244` - the `JoinStep` branch is skipped and the node hashes as the
  combination of its children, so two operators over the same inputs with different kinds or
  predicates collide. This is inside the pass's stated best-effort contract (a collision can only
  make Auto-PR reuse a slightly-off estimate) and is not new: a `JoinStepLogical` that is not a
  parallel hash join already hashes exactly this way, and `setAggregationHashTableCacheKeys` runs
  before physicalization, so an aggregation's key never sees the operator at all.
- `considerEnablingParallelReplicas.cpp:175` - `findReadingStep` returns `nullptr` at any two-child
  step that is not a `JoinStep`, so Auto-PR skips rather than descending into the wrong side.
- ⚠️ `ParallelReplicasLocalPlan.cpp:53,100` is the one traversal that does *not* skip an unrecognised
  two-child step - it descends into `children.at(0)`, which for a `RIGHT` join would pick the wrong
  side. It is unreachable for the operator: both `findReadingStep` and `findReadingSteps` run on the
  plan `InterpreterSelectQueryAnalyzer::extractQueryPlan` returns, which is not yet optimized, so the
  join there is still a `JoinStepLogical` and the existing `join_logical` branch handles it. Left
  as it is rather than guarded, because a blanket "stop at any unknown multi-child step" would also
  stop at `CreatingSetsStep`, which the traversal must descend. If either traversal is ever moved
  after optimization it needs a case for the operator.
- `removeRedundantSorting.cpp:287` needed no companion case. The `IEJoinStep` entry is there because
  an IEJoin merges pre-sorted inputs that the planner inserts for it; the block nested loop operator
  requires no sorted input and the planner inserts no sorting for it, so a sorting below it that an
  `ORDER BY` above it makes redundant is genuinely redundant. The new test pins that it is removed.
- Passes outside the Context list that run after physicalization were checked the same way and all
  degrade by producing nothing for the step: `applyOrder` (not an `ITransformingStep` or `UnionStep`,
  so no sorting property is advertised above the operator), `applyStreamDisjointness` and
  `limitPushDown` (both require a single child), and `optimizeUseNormalProjections` /
  `optimizeUseAggregateProjections` (`QueryDAG::build` rejects any step with more than one child).

### Task 9: `EXPLAIN` output and documentation

- [x] finalise `describeActions` so `EXPLAIN actions = 1` prints the join kind, strictness and the
      predicate expression in a stable, readable form
- [x] update `docs/reference/sql-reference/statements/select/join.mdx` to state that an arbitrary `ON`
      condition is supported for every kind and strictness, and that it executes as a nested loop with
      the performance implications that carries
      (the file now lives at `docs/reference/statements/select/join.mdx`, slug unchanged)
- [x] check whether the doc's existing claims about inequality support and `join_use_nulls` are still
      accurate and correct them if not
- [x] write an `EXPLAIN actions = 1` test pinning the step description
      (`tests/queries/0_stateless/04824_block_nested_loop_join_explain.sql.j2`)
- [x] run tests - must pass before task 10

**Decisions recorded in task 9**

- The step describes itself as `Type`, `Strictness` and `Condition`, the shape `IEJoinStep` and the
  legacy `JoinStep` description use. `Condition` names the condition column in the legacy format -
  the analyzer expression that computes it, `less(__table1.x, __table2.y)` - and is rendered by
  `QueryPlanFormat::formatNodePretty` in the pretty one, `x < y`. The pretty rendering cannot go
  through `formatColumnPretty`: the condition is computed inside the operator, so its name is not in
  the plan's pretty-name map, and the sub-DAG has to be formatted directly.
- `actions = 1` additionally dumps the predicate's `ExpressionActions` in the legacy format, next to
  the condition column, as `FilterStep` does for the column it filters on. The pretty format is
  `compact` by default and prints no action dump for any step, so the rendered condition is all it
  says. The JSON overload adds the same dump as an `Expression` tree.
- The doc claim "the inequality conditions are not supported with `join_use_nulls`" was stale: the
  restriction is in `PlannerJoins::buildJoinClausesAndActions`, which nothing calls any more. The
  claim "supported only for `hash` and `grace_hash`" is still true, but only of a *mixed* condition
  (an equality next to the inequality); the doc now says which case each claim is about.
- ⚠️ `04821_block_nested_loop_join_spill` was flaky (4 failures in 20 runs, always `RIGHT SEMI`): it
  compared `groupArray((l.id, r.id))` between a spilled and an unspilled run, but `RIGHT SEMI` and
  `LEFT ANY` take one pair per row of their driving side and which pair that is stays open - the
  probe streams race for a build row. Those two kinds now compare only their driving side's rows;
  25 further runs pass.

### Task 10: Verify acceptance criteria

- [x] verify no `JOIN ON` expression is rejected with "cannot determine join keys" for any supported
      kind/strictness
- [x] verify the plan and results of every query that worked before this change are unchanged
      (`INNER`/`CROSS` non-equi still uses `CROSS` + filter; disjunctive still uses hash clauses)
- [x] verify peak memory on a large non-equi join is bounded by build side + one tile, by comparing
      `MemoryTracker` peak against the `CROSS JOIN` + `WHERE` rewrite
- [x] run the join-related tests, only local one that do not require any special setop (parallel replicas, integration tests or have any special requirements in tags) and verify they pass
- [x] run linter and the style check - all issues must be fixed
- [x] run the build with no new warnings

**Decisions recorded in task 10**

- **No `JOIN ON` expression is rejected for the reason "cannot determine join keys" any more.** Two
  sweeps, 408 queries in all, none of which produced that message. The first is the kind/strictness
  matrix: 12 supported pairs x 11 keyless conditions (`<`, `!=`, `a + b > c`, `BETWEEN`, a
  non-monotone function, a function of both sides, a disjunction with a keyless disjunct, a
  conjunction of two inequalities, a hash predicate, a constant-false condition, an `IS NULL`
  disjunction) x `query_plan_join_swap_table` both ways. The second is 9 kind/strictness pairs x 16
  exotic condition shapes - single-side-only, literal `true`, `Tuple`, `Dynamic`, `Variant`,
  `LowCardinality`, `Array`, a `DateTime` interval, a lambda, a `Nullable`-returning condition, and
  an `IN` over a tuple of both sides - and every one of the 144 returned a result.
- ⚠️ `ANY FULL JOIN` is the one combination `isSupportedJoinType` admits that no query can reach:
  `QueryTreeBuilder.cpp:1155` rejects it with `NOT_IMPLEMENTED` while the query tree is being built,
  for an equi join exactly as for a keyless one. It is a pre-existing global restriction, not a
  limitation of the operator, so `isSupportedJoinType` is left as the superset.
- **Nothing that worked before changed.** On the plan side, `EXPLAIN` still shows: `INNER`/`CROSS`
  non-equi as `cross` + `ConstantJoin` + a `Filter`, a disjunction whose every disjunct has keys as a
  `HashJoin` with `a = b OR id = id`, a plain equi join as `SpillingHashJoin`, a mixed
  equality-plus-inequality as a hash join, two inequalities under `join_algorithm = 'ie_join'` as
  `IEJoin`, and `ASOF` as an asof hash join. On the result side, the diff against `master` touches
  three pre-existing source files (`JoinStepLogical.cpp`, and `QueryPipelineBuilder.{h,cpp}` for one
  new method with no existing caller changed), and every new branch in `buildPhysicalJoinImpl` is
  guarded by `use_block_nested_loop`, which can only be set where the code used to throw. Of the 13
  pre-existing tests modified, every changed line is a query that previously carried
  `{ serverError INVALID_JOIN_ON_EXPRESSION }`; no reference line for a query that already returned a
  result was edited.
- **Peak memory is flat in the probe side and in the size of the cartesian product.** With
  `max_threads = 1`, the same `INNER` non-equi join routed to the operator (by disabling `hash`, so
  `can_convert_to_cross` is false) against a 1000-row build side reports a `MemoryTracker` peak of
  4.26 MiB at 100 000, 500 000 and 2 000 000 probe rows - a 20x growth in probe rows and a 20x growth
  in evaluated pairs at a constant peak. Holding the probe side at 20 000 rows and growing the build
  side 1 000 -> 10 000 -> 100 000 rows (0.2 -> 20 MB of payload, 1.95 billion pairs evaluated) keeps
  the peak between 2.6 and 4.4 MiB, because the store compresses past
  `cross_join_min_rows_to_compress`. The `CROSS JOIN` + `WHERE` rewrite returns *identical results*
  in every one of those runs at 0.37-1.12 MiB: it holds the same build side but no tile, so the
  operator's overhead over it is the tile, as intended. Without `max_threads = 1` the peak grows with
  the thread count, not with the input - one probe chunk plus one tile per probe stream.
- A deliberately hostile shape - a 100 000-row build side and a per-row always-true predicate over
  500 probe rows, 50 million output rows - peaks at 5.08 MiB under a 400 MiB `max_memory_usage` cap,
  so the operator degrades to a slow but bounded cartesian product rather than an OOM. A condition
  that folds to a constant is short-cut by `ConstantFilterDescription` and costs no per-pair work at
  all.
- The `join`-matching stateless suite (1010 tests) was re-run: 987 pass, and the 23 failures are the
  same environment-limited set recorded in tasks 5 and 9 - 7 need `process_query_plan_packet` in the
  server config (`pr_plan_based_join_*`, `04201_max_bytes_ratio_before_external_join_distributed`),
  14 need the stateful `hits`/`visits` datasets, `03325_sqlite_join_wrong_answer` needs `sqlite3`, and
  `03800_s3_cluster_join` needs a local MinIO. All 12 block nested loop tests pass, and so do the 54
  gtests in `gtest_block_nested_loop_join_{data,probe}.cpp`.
- The repository style check passes every check that inspects the working tree - `cpp`,
  `whitespace_check`, `catch_all`, `functional_tests_check`, `test_numbers_check` (no numbering
  collision for 04813-04824), `ruff`, `yamllint`, `xmllint`, `embedded_doc_snippets` - with zero
  findings in any file this branch touches. Its three failures are local-environment artifacts:
  broken symlinks under `build*/cargo` and `tmp/`, `settings_changes_history` refusing to run without
  the PR workflow hook that records the changed files, and the `various` check tripping on `find`
  writing "Permission denied" to stderr for root-owned `tests/integration/*/_instances-*` leftovers.
- The build is clean under the project-wide `-Werror`: a full `ninja clickhouse` and a
  `ninja unit_tests_dbms` produce no `warning:` and no `error:` line, and the seven changed
  translation units were force-recompiled from scratch to confirm it rather than relying on a cached
  object.

### Task 11: [Final] Update documentation

- [x] update this plan with the decisions recorded during implementation (`WITH TOTALS`,
      `join_any_take_last_row`/etc)
- [x] add the follow-up optimizations from "Deliberately out of scope" to the repository's issue
      tracker notes if a place for them exists (as `TODO` comments at the code sites - see below)

**Decisions recorded in task 11**

- The decisions taken while implementing were written down in each task's own section as that task
  landed, so this task only had to reconcile the parts of the plan that describe the *design* with
  what was actually built. `WITH TOTALS` is in task 3 (supported, with `JoinCommon::joinTotals`'
  semantics), `join_any_take_last_row` in task 6 (ignored on this path), the spill trigger in task 7,
  and the optimizer-pass audit in task 8.
- "Technical Details" was rewritten where it had gone stale against the code: the store keeps
  `BuildBlockEntry` rather than `StoredBlock` and hands blocks out by index through a
  `BuildSideBlockReader`; `matched_flags` also covers `ANY INNER`; the happens-before edge is
  `DelayedPortsProcessor` rather than the `FinishCounter`; the pipeline diagram now shows the two
  `DelayedPortsProcessor`s and the totals transform; the tile is bounded in pairs by
  `DEFAULT_BLOCK_SIZE` rather than by the output limit; the match mask comes from `FilterDescription`;
  and the early exit is off for the kinds that pad unmatched build rows.
- **The repository has no file for issue-tracker notes** - no `ROADMAP`, no `TODO` file, and the one
  precedent in `docs/plans/completed/` records its follow-ups in the plan's own "Post-Completion"
  section. The repository's actual convention for a deliberate limitation is a `/// TODO:` comment at
  the code site (112 of them under `src/Processors/` alone), so each of the four follow-ups from
  "Deliberately out of scope" is now stated as one there, next to the code that would change:
  tile pruning at `matchNextTile`, grace partitioning at `spillInMemoryBlocks`, and both the
  smaller-side swap and the takeover of the cross-plus-filter and disjunctive paths at the routing
  site in `buildPhysicalJoinImpl`. The list in "Deliberately out of scope" now points at each site,
  so the plan and the code do not drift apart.

*Note: ralphex automatically moves completed plans to `docs/plans/completed/`*

## Technical Details

### Data structures

`BlockNestedLoopJoinData` (`src/Processors/Transforms/BlockNestedLoopJoinData.h`) — shared between the
build transform, every probe transform, and the unmatched-emit processor:

- `std::vector<BuildBlockEntry> blocks` plus the global row offset of each block, so a global build
  row number maps to `(block, offset)` and stays stable when a block spills. An entry holds a
  `StoredBlock` (`ScatteredBlock.h:376`) that is in memory, compressed, or written out to the
  temporary file and named only by its position in it; `num_rows` is kept either way. A block is
  named by its index and read back through a `BuildSideBlockReader`, so there is no `getBlocks()`.
- `total_rows`, `total_bytes` (the whole build side, spilled blocks included, which is what
  `max_bytes_in_join` limits), `in_memory_bytes`, and `SizeLimits`.
- `matched_flags` — one `std::atomic_bool` per build row, allocated by `finish` and only for the
  kinds that need it: `RIGHT`, `FULL`, right-driven `SEMI`/`ANTI`, and `ANY INNER` (which claims a
  build row so that each row of either side is taken at most once). Both the writes and the reads are
  relaxed: a flag is only ever set, and the happens-before edge for the unmatched scan comes from the
  pipeline — a probe stream finishes its output port only after its last write, and the
  `DelayedPortsProcessor` above it observes that finish before the unmatched stage produces a row.

The predicate, mirroring `IEJoinResidualCondition` and living next to the probe transform in
`src/Processors/Transforms/BlockNestedLoopJoinTransform.h`:

```cpp
struct BlockNestedLoopPredicate
{
    struct Source { size_t side = 0; size_t position = 0; };
    ExpressionActionsPtr actions;
    std::vector<Source> inputs;  /// one per required column, in getRequiredColumnsWithTypes order
};
```

### Processing flow

```
build pipeline ─> resize(max_streams) ─> BlockNestedLoopBuildTransform x N ─> DelayedPortsProcessor ─┐
                                                    │                              (main ports)     │
                                                    v                                               │
                                       BlockNestedLoopJoinData                                      │
                                                    │                                               │
probe pipeline ─> resize(max_streams) ─> BlockNestedLoopProbeTransform x N <──(delayed ports)────────┘
                                                    │
                                                    ├─> DelayedPortsProcessor ─> UnmatchedBuildRows x N
                                                    │                            (RIGHT/FULL/RIGHT ANTI)
                                                    └─> BlockNestedLoopTotals (WITH TOTALS)
```

The build-then-probe edge is `QueryPipelineBuilder::addPipelineBefore`: the build transforms'
empty-header output ports are the main ports of a `DelayedPortsProcessor` whose delayed ports are the
probe streams and the probe totals stream. The unmatched-build-rows stage is a second
`DelayedPortsProcessor`, this time with the probe streams as its main ports — the shape
`joinPipelinesRightLeft` uses for `NonJoinedBlocksTransform`.

Per probe chunk, per stored build block:

1. Build the tile: `probe_tile[i] = ColumnReplicated(probe_col, repeat_each(i, build_rows))`,
   `build_tile[j] = ColumnReplicated(build_col, tile_of(j))` — but only where
   `isLazyReplicationUseful` says the indirection beats a gather, mirroring `replicateColumnsLazily`.
   The two sides do not share an index column, so a condition over both materialises the tile, which
   is the intended bound: one tile rather than the cartesian product.
2. Evaluate the predicate over the tile and turn the condition column into a match mask with
   `FilterDescription`, which is where "a `Nullable` result's NULL is not a match" comes from,
   together with the handling of a `LowCardinality`, `Sparse` or non-`UInt8` numeric condition.
   `ConstantFilterDescription` short-cuts a condition that folded to a constant.
3. Append the surviving `(probe row, row in block)` pairs, as many of them as the strictness selects
   (`PairSelection`); set `matched_flags[build_index]` — or claim it with an atomic `exchange` for a
   right-driven `ANY`/`SEMI` — and the per-probe-row match bit, where the kind needs them.
4. When the accumulated pair count reaches `max_block_size` (`max_joined_block_size_rows`) or the byte
   estimate reaches `max_block_bytes` (`max_joined_block_size_bytes`), materialise one output chunk
   and yield; the cursor state resumes the walk on the next `work()`. An output chunk may span several
   stored blocks, so the pairs carry a `BuildRun` per block, which keeps that block alive — the walk
   may have passed it, and a compressed or spilled block would otherwise be read back twice.
   `work` also yields with no output every `8 * max(DEFAULT_BLOCK_SIZE, max_block_size)` evaluated
   pairs, so a walk that matches nothing stays cancellable.
5. For a left-driven `ANY`/`SEMI`/`ANTI`, a probe row that has matched is excluded from subsequent
   tiles; when every probe row in the chunk has matched, the build-side walk stops. This early exit is
   off for the kinds that pad the build rows nothing matched (`RIGHT`/`FULL` with the old `ANY`, and
   `RIGHT ANTI`), because a probe row that stopped early would leave build rows it would have matched
   unflagged and the scan after the probe would emit them as unmatched.

Unmatched rows are emitted by inserting each padded column's default, because `addToNullableIfNeeded`
(`JoinStepLogical.cpp:175`) already made those columns `Nullable` in the pre-join actions when
`join_use_nulls` requires it.

### Tile sizing

The tile is sized in pairs — `DEFAULT_BLOCK_SIZE` of them — independently of the output limit: it
bounds the intermediate columns the condition is evaluated on, while the output chunk is cut to
`max_block_size` when the accumulated pairs are materialised. A build block is therefore walked in
sub-ranges of `max(1, DEFAULT_BLOCK_SIZE / probe_rows)` rows, so the tile never grows with the size of
a build block.

## Post-Completion

*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification**:

- Run the performance suite to confirm no regression on queries that already worked; BNL is additive,
  so any perf change on an existing query indicates an accidental takeover of a working path.
- Sanity-check memory behaviour under a deliberately hostile query (large build side, always-true
  predicate) to confirm the operator degrades to a slow but bounded cartesian product rather than an
  OOM.

**External updates**:

- Push `vdimir/block-nested-loop-join` and open a PR targeting `master`, using
  `.github/PULL_REQUEST_TEMPLATE.md`; changelog category "New Feature".
- Reference `docs/plans/20260729-harden-constant-join-predicate.md` and branch
  `vdimir/outer_join_inequality_to_cross` in the PR description as the superseded approach, and close
  or re-scope draft PR https://github.com/ClickHouse/ClickHouse/pull/108960 accordingly.
- Watch CI; analyse failures with `.claude/tools/fetch_ci_report.js`.
