# Audit: `DelayedPortsProcessor` wiring for materialized CTE readers

Date: 2026-05-26
Branch: `fix-mat-cte`
Scope: Pre-requisite for removing the `std::promise<void>` / `std::shared_future<void>`
synchronisation between `MaterializingCTETransform` (writer) and the `MemorySource`
created inside `ReadFromMemoryStorageStep::makePipe` (reader).

## Goal

For every code path that creates a `MemorySource` reading a materialized CTE's
`StorageMemory`, prove that the source is downstream of a `DelayedPortsProcessor`
whose "main" inputs include the corresponding `MaterializingCTETransform`. If any
gap is found, the plan stops and the planner is fixed before we touch the
runtime synchronisation.

## Background: how the gate is built

The skeleton chain is identical for every reader path:

1. The planner produces a `DelayedMaterializingCTEsStep` somewhere on top of
   the consumer plan. The step owns the *pre-built* `QueryPlan` for the CTE
   (its root is a `MaterializingCTEStep`) — `src/Planner/Planner.cpp:1694-1734`,
   inside `addBuildSubqueriesForMaterializedCTEsIfNeeded`.
2. `QueryPlan::optimize` calls `resolveMaterializingCTEs` once
   (`src/Processors/QueryPlan/QueryPlan.cpp:734-735`). That function does two
   traversals (`src/Processors/QueryPlan/Optimizations/addPlansForMaterializingCTEs.cpp:91-130`):
   pass 1 invokes `DelayedMaterializingCTEsStep::optimizePlans` so each CTE's
   own plan is optimised (and any recursive inplace materialisation through
   `buildSetInplace` claims the CTE first); pass 2 visits the same nodes and
   rewrites each surviving `DelayedMaterializingCTEsStep` into a
   `MaterializingCTEsStep` whose children are `(consumer_subtree,
   cte_plan_root_1, cte_plan_root_2, ...)`
   (`src/Processors/QueryPlan/Optimizations/addPlansForMaterializingCTEs.cpp:12-39`).
3. `MaterializingCTEsStep::updatePipeline`
   (`src/Processors/QueryPlan/MaterializingCTEStep.cpp:69-96`) takes the
   consumer's pipeline as `main_pipeline` and the union of CTE pipelines as
   `delayed_pipeline`, then calls
   `main_pipeline->addPipelineBefore(std::move(delayed_pipeline))`
   (line 91).
4. `QueryPipelineBuilder::addPipelineBefore`
   (`src/QueryPipeline/QueryPipelineBuilder.cpp:826-856`) builds a
   `DelayedPortsProcessor`. Inputs come from both pipelines after
   `Pipe::unitePipes`. The `delayed_streams` vector covers exactly the *main*
   consumer output ports (positions `0 .. pipe.numOutputPorts() + num_extra_ports - 1`).
   The remaining inputs — the CTE pipeline tails ending in
   `MaterializingCTETransform` (`addMaterializingCTETransform`,
   `src/QueryPipeline/QueryPipelineBuilder.cpp:809-824`) — are passed as
   "main" (non-delayed) inputs with empty headers (the assertion is enforced
   by `assert_main_ports_empty = true`).
5. `DelayedPortsProcessor::prepare`
   (`src/Processors/DelayedPortsProcessor.cpp:125-182`) gates the delayed
   inputs: `shouldSkipDelayed()` returns `true` while
   `num_finished_main_inputs + num_delayed_ports < port_pairs.size()` (line
   120-123), and the code path at lines 147 / 163 explicitly refuses to call
   `setNeeded` / `processPair` on delayed inputs while it returns true. The
   back-pressure cascades upward into every reader transform in the main
   pipeline, including the `MemorySource` instances created by
   `ReadFromMemoryStorageStep`.

The proof of "every materialized-CTE `MemorySource` goes through this gate"
therefore reduces to: prove the planner always produces, somewhere above the
reader, a `DelayedMaterializingCTEsStep` (or already a `MaterializingCTEsStep`)
that names the CTE.

A subtlety worth stating up front: `ReadFromMemoryStorageStep::makePipe`
(`src/Processors/QueryPlan/ReadFromMemoryStorageStep.cpp:217-264`) takes the
`delay_read_for_global_sub_queries` branch *only* for CTE readers — the
non-delayed branch (line 250-263) does not attach a `MaterializedCTEPtr` to
the `MemorySource`. The non-delayed branch is dead code for CTE storages
because every `TemporaryTableHolder` created for a materialized CTE is
constructed with `create_for_global_subquery = true` and so
`StorageMemory::delayReadForGlobalSubqueries` always fires for them
(`src/Analyzer/Resolve/QueryAnalyzer.cpp:3091-3098`,
`src/Interpreters/DatabaseCatalog.cpp:177-187`). Conclusion: there is exactly
one `MemorySource` construction site to audit — line 238-247 — and the
question is whether *every* such construction is downstream of a
`DelayedPortsProcessor` named on the same CTE.

---

## Path 1 — Regular `FROM cte_name` in the outer query

* **`MemorySource` creator.** `ReadFromMemoryStorageStep::makePipe`,
  `src/Processors/QueryPlan/ReadFromMemoryStorageStep.cpp:238-247`, called
  from `StorageMemory::readImpl`,
  `src/Storages/StorageMemory.cpp:204-216`.
* **`MaterializingCTETransform` creator.** `MaterializingCTEStep::transformPipeline`,
  `src/Processors/QueryPlan/MaterializingCTEStep.cpp:49-52`, which is the
  root step of every CTE plan produced by
  `addBuildSubqueriesForMaterializedCTEsIfNeeded`
  (`src/Planner/Planner.cpp:1720-1725`).
* **`addPipelineBefore` site.** `MaterializingCTEsStep::updatePipeline`,
  `src/Processors/QueryPlan/MaterializingCTEStep.cpp:91`.
* **Where the `DelayedMaterializingCTEsStep` is planted.** `Planner::buildPlanForQueryNode`
  reaches the top-level `addBuildSubqueriesForMaterializedCTEsIfNeeded`
  call at `src/Planner/Planner.cpp:2605` (after `collectMaterializedCTEs`
  at line 2152 has discovered every `TableNode` whose `getMaterializedCTE()`
  is non-null). The step lives above the consumer's plan; pass 2 of
  `resolveMaterializingCTEs` rewrites it into a `MaterializingCTEsStep`
  whose consumer subtree contains the `ReadFromMemoryStorageStep`.
* **What proves they're connected.** The `MaterializingCTEsStep` is the
  immediate parent of the `ReadFromMemoryStorageStep`'s subtree (it wraps
  whatever the original top of the consumer plan was), and
  `addPipelineBefore` puts the consumer's outputs in the `delayed_streams`
  set while the writer pipeline (whose tail is `MaterializingCTETransform`)
  becomes the non-delayed main input — exactly what
  `DelayedPortsProcessor::shouldSkipDelayed()` keys on.

No gap.

---

## Path 2 — `cte_name` inside an `IN (subquery)` consumed by primary-key index analysis

* **`MemorySource` creator.** Same as Path 1 — the IN-subquery's plan is
  ultimately executed by `FutureSetFromSubquery::buildOrderedSetInplace`,
  `src/Interpreters/PreparedSets.cpp:360-419`, which calls
  `plan->buildQueryPipeline(...)` (line 397). Wherever the subquery
  references the CTE, a `ReadFromMemoryStorageStep` sits in that plan and
  hits `makePipe` line 238-247 at pipeline build time.
* **`MaterializingCTETransform` creator.** Same as Path 1 —
  `MaterializingCTEStep::transformPipeline` plants the transform inside
  the CTE plan built by `addBuildSubqueriesForMaterializedCTEsIfNeeded`.
  Which top-level call plants it depends on whether the inplace path or
  the outer pipeline wins the
  `is_materialization_planned.exchange(true)` race
  (`src/Processors/QueryPlan/MaterializingCTEStep.cpp:144`,
  `DelayedMaterializingCTEsStep::makePlansForCTEs`).
* **`addPipelineBefore` site.** `MaterializingCTEsStep::updatePipeline`,
  `MaterializingCTEStep.cpp:91`, on whichever plan ends up issuing the
  pipeline.
* **Safety-net `DelayedMaterializingCTEsStep` site.** The IN-subquery's
  plan is built by `addBuildSubqueriesForSetsStepIfNeeded`
  (`src/Planner/Planner.cpp` around 1587-1623). Critically, line 1603 sets
  `subquery_options.forceMaterializeCTE()` on the subquery's
  `SelectQueryOptions` before constructing the inner `Planner`. That call
  flips `force_materialize_cte` so that `collectMaterializedCTEs`
  (`src/Planner/CollectMaterializedCTE.cpp:23-26`) does not short-circuit
  on the `is_subquery` check. The inner Planner then reaches its own
  `addBuildSubqueriesForMaterializedCTEsIfNeeded` call (line 2605 — the
  same call from `buildPlanForQueryNode` — also reachable from
  `buildPlanForUnionNode` line 2018) and plants one
  `DelayedMaterializingCTEsStep` per CTE level on the subquery plan.
* **What proves they're connected.** Two flows are possible and both
  preserve the gate:
  1. **Inplace materialisation wins the race.** Pass 1 of the outer
     `resolveMaterializingCTEs` invokes
     `DelayedMaterializingCTEsStep::optimizePlans`
     (`addPlansForMaterializingCTEs.cpp:57-87`), which calls
     `cte->plan->optimize(...)`. Inside the CTE plan,
     `addStepsToBuildSets` (called by `QueryPlan::optimize` at
     `QueryPlan.cpp:732-733`) may detect that an inner read requires the
     IN set inplace and trigger
     `FutureSetFromSubquery::buildOrderedSetInplace`
     (`PreparedSets.cpp:360-419`). That call recursively builds the
     subquery plan and runs `plan->optimize(...)`. The recursive
     `resolveMaterializingCTEs` then encounters the safety-net
     `DelayedMaterializingCTEsStep` planted by `forceMaterializeCTE`,
     wins `is_materialization_planned.exchange(true)`, and converts the
     safety-net into a `MaterializingCTEsStep` *inside the subquery
     plan*. From here the chain is identical to Path 1: the subquery's
     `MemorySource` is downstream of a `MaterializingCTEsStep` →
     `DelayedPortsProcessor`. The outer
     `DelayedMaterializingCTEsStep::makePlansForCTEs` finds the plan
     has already been moved out (the comment at MaterializingCTEStep.cpp:129-135
     describes this) and produces no additional step for that CTE.
  2. **Outer materialisation wins the race.** The IN-set is constructed
     at runtime via the regular `CreatingSetsStep` pipeline. The
     subquery plan's safety-net is then stripped: when
     `DelayedCreatingSetsStep::makePlansForSets` builds the runtime plan
     it calls `removeTopLevelDelayedMaterializingCTEsStep(*plan)`
     (`src/Processors/QueryPlan/CreatingSetsStep.cpp:237`). Stripping
     is safe in this branch because the outer plan's
     `MaterializingCTEsStep` (from the top-level
     `addBuildSubqueriesForMaterializedCTEsIfNeeded`) already wraps the
     main consumer pipeline — and the `CreatingSetsStep` is part of
     that consumer pipeline, hence downstream of the outer
     `DelayedPortsProcessor`. The comment at CreatingSetsStep.cpp:220-236
     spells out exactly this invariant.

No gap.

---

## Path 3 — `cte_name` referenced inside multiple UNION branches

* **`MemorySource` creator.** Same as Path 1, inside each UNION branch's
  subplan.
* **`MaterializingCTETransform` creator.** Same as Path 1 — the
  transform lives on the CTE's pre-built plan.
* **`addPipelineBefore` site.** `MaterializingCTEsStep::updatePipeline`,
  `MaterializingCTEStep.cpp:91`. The interesting question is **which**
  `MaterializingCTEsStep` runs — there may be more than one
  `DelayedMaterializingCTEsStep` referencing the same CTE.
* **Safety-net stack site.** `Planner::buildPlanForUnionNode` reaches
  `addBuildSubqueriesForMaterializedCTEsIfNeeded` at
  `src/Planner/Planner.cpp:2018`. The comment block at lines 2006-2014
  documents the race we fix: each child of the UNION/INTERSECT/EXCEPT
  independently plants its own `DelayedMaterializingCTEsStep`, but only
  one wins the atomic `is_materialization_planned` race in
  `makePlansForCTEs` (line 144 of `MaterializingCTEStep.cpp`). Without
  the union-level step, the losing siblings would read from the CTE
  while the winning sibling is still materialising it. The fix is the
  union-level `DelayedMaterializingCTEsStep` planted by
  `addBuildSubqueriesForMaterializedCTEsIfNeeded` *above* the
  `UnionStep`/`IntersectOrExceptStep` (the call at line 2018, right
  after the union step is added at line 1989-2003 inside the same
  function body).
* **What proves they're connected.**
  1. Pass 2 of `resolveMaterializingCTEs` walks the plan
     top-down via the stack at `addPlansForMaterializingCTEs.cpp:111-129`
     and calls `addPlansForMaterializingCTEs` post-order? Actually
     **pre-order**: the function is invoked once per node before children
     are visited. So the **outer** (union-level)
     `DelayedMaterializingCTEsStep` is processed first and wins
     `is_materialization_planned.exchange(true)` for every CTE it owns.
     It becomes a `MaterializingCTEsStep` whose `main_pipeline` is the
     entire `UnionStep` / `IntersectOrExceptStep` subtree — every
     branch's `MemorySource` is downstream.
  2. The per-branch `DelayedMaterializingCTEsStep` nodes are still
     visited, but `makePlansForCTEs` (line 139-156 of
     `MaterializingCTEStep.cpp`) returns an empty `plans` vector for the
     already-claimed CTEs (the `exchange(true)` returns `true`, so the
     plan move is skipped). The per-branch step is rewritten into a
     `MaterializingCTEsStep` with only one input — the branch consumer
     — and `updatePipeline` line 75-76 returns `main_pipeline`
     unchanged (degenerate case, no `addPipelineBefore` call). So the
     redundant per-branch wrappers are no-ops at pipeline build time
     and do not interfere with the union-level gate.

No gap.

---

## Path 4 — Distributed queries (parallel replicas / `RemoteQueryExecutor`)

This path has two flavours: the initiator side and the remote side. The
audit focuses on the remote side because that's where `MemorySource`
instances are constructed.

* **Initiator side: external table delivery.** When a remote query is
  about to start, `RemoteQueryExecutor::sendExternalTables`
  (`src/QueryPipeline/RemoteQueryExecutor.cpp:921-`) iterates over the
  external `StorageMemory` tables it would ship to the shard. Lines 949-957
  contain the crucial check:

  ```cpp
  auto materialized_cte = storage_memory->getMaterializedCTE();
  if (materialized_cte != nullptr && !materialized_cte->isBuilt())
  {
      LOG_DEBUG(log, "Skipping sending CTE '{}' ...", materialized_cte->cte_name);
      continue;
  }
  ```

  Interpretation: if the initiator has *not yet* materialised the CTE
  (the typical case for parallel-replicas, where the CTE materialisation
  itself runs on replicas and we must avoid the circular dependency
  described in the comment), do not send the `StorageMemory`. Otherwise
  (CTE already built locally — sequential or single-replica case),
  the local in-memory CTE blocks are shipped as an external table just
  like any other `StorageMemory`. This **does not** weaken the
  reader-side guarantee on either flavour:

  * If the table *is* sent, the remote side reads it via a normal
    external-table `ReadFromMemoryStorageStep` whose `StorageMemory` is
    a fresh shard-local copy with no `MaterializedCTEPtr` attached
    (`getMaterializedCTE()` returns null on the shard-side storage —
    `setMaterializedCTE` is only called inside
    `TableNode::finalizeMaterializedCTE`, which runs on the initiator).
    Hence the `delay_read_for_global_sub_queries` branch in
    `ReadFromMemoryStorageStep::makePipe` constructs a `MemorySource`
    whose `materialized_cte_` argument is empty (line 247 calls
    `getMaterializedCTE()` which returns null), so `generate()` does
    not wait on any future — there's nothing to wait for, the table is
    already populated.
  * If the table is *not* sent, the remote side has to rebuild the CTE
    locally. The shard-side query tree (produced by
    `buildQueryTreeForShard`, `src/Storages/buildQueryTreeForShard.cpp:566-`)
    contains the CTE `TableNode` with its own `MaterializedCTE`
    instance, and the secondary-query Planner runs the full pipeline:
    `collectMaterializedCTEs` → `addBuildSubqueriesForMaterializedCTEsIfNeeded`
    → `DelayedMaterializingCTEsStep` → `resolveMaterializingCTEs` →
    `MaterializingCTEsStep` → `addPipelineBefore` →
    `DelayedPortsProcessor`. Identical to Path 1. So the remote-side
    `MemorySource` is gated by the remote-side `DelayedPortsProcessor`.
* **`buildQueryTreeForShard.cpp:421` use of `forceMaterializeCTE`.** That
  call sits inside `executeSubqueryNode`
  (`src/Storages/buildQueryTreeForShard.cpp:403-`), which is the path
  used to materialise a `GLOBAL IN` / `GLOBAL JOIN` subquery into a
  temporary table before forwarding the rewritten query to the shard.
  Same shape as Path 2 — the inner subquery may reference a CTE, the
  safety-net `DelayedMaterializingCTEsStep` planted by
  `forceMaterializeCTE` becomes a `MaterializingCTEsStep` either inplace
  or at the subquery's pipeline construction time, and the
  `MemorySource` reading the CTE is downstream of its
  `DelayedPortsProcessor`.
* **`StorageMergeTreeAnalyzeIndexes.cpp:161` use of `forceMaterializeCTE`.**
  This is the same shape as Path 2 — index-analysis path that builds
  IN-subquery sets inplace for a parts-pruning analysis. The
  `forceMaterializeCTE` on `subquery_options` (line 161) makes each set
  subquery's Planner plant the safety-net
  `DelayedMaterializingCTEsStep`, which the subquery's own
  `resolveMaterializingCTEs` then rewrites into a `MaterializingCTEsStep`
  inside the subquery plan. Same gating chain.

No gap. The "skip sending CTE when not built" branch in
`RemoteQueryExecutor.cpp` is part of the remote-rebuild flavour — it
is what lets the remote side run its own planner, plant its own
`DelayedMaterializingCTEsStep`, and thereby gate its own `MemorySource`.

---

## Path 5 — `removeTopLevelDelayedMaterializingCTEsStep` call sites

The only call site is `src/Processors/QueryPlan/CreatingSetsStep.cpp:237`,
inside `DelayedCreatingSetsStep::makePlansForSets`. The function strips
*all* consecutive `DelayedMaterializingCTEsStep` nodes from the top of
the set-subquery's plan (the loop at
`src/Processors/QueryPlan/MaterializingCTEStep.cpp:193-207`), optionally
skipping one wrapper (e.g. `CreatingSetStep` planted by
`FutureSetFromSubquery::build`, `src/Interpreters/PreparedSets.cpp:305-323`).

Why is stripping safe? The contract documented at
`CreatingSetsStep.cpp:220-236` is: this code path runs only when the
set is going to be built at **runtime** (not inplace). The runtime
construction pipeline is part of the outer query's main pipeline —
specifically, it lives downstream of the outer
`DelayedCreatingSetsStep` and inside the same query plan that
`addBuildSubqueriesForMaterializedCTEsIfNeeded` has already wrapped
with a top-level `DelayedMaterializingCTEsStep` (planted at
`Planner.cpp:2605` for query nodes, line 2018 for union nodes). So
when the outer `resolveMaterializingCTEs` runs, it converts that
top-level step into a `MaterializingCTEsStep`, and the entire runtime
set-construction pipeline — including this stripped subquery plan —
ends up as the consumer side of the outer `DelayedPortsProcessor`.

The single dangerous mistake the function avoids: stripping safety-net
steps deep inside **nested** `DelayedCreatingSetsStep` source plans
would unhood `MemorySource` instances that *do* need inplace
materialisation. The comment at `CreatingSetsStep.cpp:232-236`
explicitly forbids recursion into nested source plans; the loop in
`removeTopLevelDelayedMaterializingCTEsStep` only walks the contiguous
top-of-plan chain.

I verified by reading the loop body (`MaterializingCTEStep.cpp:193-207`):
the iteration follows `target = next` only as long as `next` is itself a
`DelayedMaterializingCTEsStep`. It stops at the first non-safety-net
step. Multiple stacked safety-nets (the case where a CTE chain crosses
several `ctes_by_level` entries from
`addBuildSubqueriesForMaterializedCTEsIfNeeded`, line 1694-1734) are
all stripped because they sit at the top of the plan in a contiguous
chain — exactly what the docstring at lines 159-173 explains.

**Stripped reader still gated?** Yes. The `MemorySource` instances in
the stripped subquery plan see no in-subquery gate, but the entire
subquery plan is the body of a `CreatingSetStep` (planted by
`FutureSetFromSubquery::build` at PreparedSets.cpp:315-321) which runs
**inside** the outer pipeline. That outer pipeline is the consumer side
of the outer `MaterializingCTEsStep` → `DelayedPortsProcessor`, which
gates **all** of its downstream — including the `CreatingSetsStep`'s
internal pipeline — until the writer is done.

No gap.

---

## Other `forceMaterializeCTE` call sites (sanity-check completeness)

`grep -rn forceMaterializeCTE src/` returns five primary call sites
(the rest are docstrings/comments referencing the function):

| File:Line | Path covered |
| --- | --- |
| `src/Planner/Planner.cpp:1603` | Path 2 (IN-subquery sets for primary-key analysis) |
| `src/Storages/buildQueryTreeForShard.cpp:421` | Path 4 (distributed GLOBAL IN/JOIN inner subquery) |
| `src/Storages/StorageMergeTreeAnalyzeIndexes.cpp:161` | Path 4 (distributed parts-analysis IN-subqueries) |
| `src/Analyzer/Resolve/evaluateScalarSubqueryIfNeeded.cpp:156` | Scalar subqueries — handled identically to Path 2 |

The scalar-subquery site (`evaluateScalarSubqueryIfNeeded.cpp:154-156`)
is a self-contained `InterpreterSelectQueryAnalyzer::execute` invocation
(line 164 of the same file) that builds and executes a standalone
pipeline. Inside that pipeline, `addBuildSubqueriesForMaterializedCTEsIfNeeded`
fires (via `Planner::buildPlanForQueryNode`/`buildPlanForUnionNode`) and
plants a `DelayedMaterializingCTEsStep` for every referenced CTE because
`force_materialize_cte` is set and `collectMaterializedCTEs` therefore
returns non-empty for `is_subquery` plans
(`src/Planner/CollectMaterializedCTE.cpp:25-26`). The pipeline's own
`resolveMaterializingCTEs` then rewrites the step into a
`MaterializingCTEsStep` and the `addPipelineBefore` →
`DelayedPortsProcessor` chain runs as in Path 1. No gap.

---

## Reader / writer storage proof

A final invariant worth recording: every `MemorySource` that today
consults `materialized_cte->build_future` (the synchronisation we want
to remove) is constructed exactly at
`src/Processors/QueryPlan/ReadFromMemoryStorageStep.cpp:238-247`, inside
the `delay_read_for_global_sub_queries` branch. Two facts prove there is
no second construction site for CTE readers:

* The non-delayed branch at lines 250-263 constructs `MemorySource`
  without a `MaterializedCTEPtr` argument (it uses the 4-argument
  constructor — line 258).
* Every materialized CTE's `StorageMemory` is built through
  `TemporaryTableHolder` with `create_for_global_subquery = true`
  (`src/Analyzer/Resolve/QueryAnalyzer.cpp:3091-3098`), and that
  parameter unconditionally calls
  `storage->delayReadForGlobalSubqueries()`
  (`src/Interpreters/DatabaseCatalog.cpp:177-187`). So the
  `delay_read_for_global_subqueries` flag on a CTE storage is always
  `true` and `makePipe` always takes the gated branch.

This means the `build_future` removal we're planning has *one* call site
to delete and *one* reader code path to verify — and the verification is
this audit document.

---

## Conclusion

**No gaps found.** Every code path that creates a `MemorySource` reading
a materialized CTE's `StorageMemory` is downstream of a
`DelayedPortsProcessor` whose main inputs include the corresponding
`MaterializingCTETransform`:

| Path | Gate location |
| --- | --- |
| 1. Regular `FROM cte_name` | Top-level `MaterializingCTEsStep` planted by `Planner.cpp:2605` |
| 2. `IN (subquery)` used in PK analysis | Either inplace `MaterializingCTEsStep` inside the subquery plan, or outer `MaterializingCTEsStep` (CreatingSetsStep.cpp:220-236 invariant) |
| 3. UNION branches | Union-level `MaterializingCTEsStep` planted by `Planner.cpp:2018` (claims CTEs pre-order before children are visited) |
| 4. Distributed remote rebuild | Shard-side `MaterializingCTEsStep` produced by the secondary query's own planner |
| 5. Stripped safety-net (`CreatingSetsStep.cpp:237`) | Outer `MaterializingCTEsStep` still wraps the runtime set construction pipeline |

The synchronous `std::shared_future<void>` wait in
`MemorySource::generate` (`ReadFromMemoryStorageStep.cpp:87-115`) is
therefore redundant for correctness — the topology-level
`DelayedPortsProcessor` is the authoritative gate, and the future
merely provided a belt-and-braces second mechanism.

Pre-requisite satisfied. Removal of `build_promise` / `build_future`
may proceed in subsequent tasks of the plan.
