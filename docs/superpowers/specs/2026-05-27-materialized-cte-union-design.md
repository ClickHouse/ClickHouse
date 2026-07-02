# Design: Deduplicate materialized CTEs across UNION branches

Status: Approved (brainstorm), pending plan.
Owner: Dmitry Novik (branch `fix-mat-cte`).
Date: 2026-05-27.

## Problem

A `MATERIALIZED` CTE referenced from multiple branches of a `UNION` is silently
duplicated: each branch ends up with its own `MaterializedCTE` object, its own
`StorageMemory`, its own `temporary_table_name`, and (worse) gets inlined
as a plain subquery instead of being materialized once and shared.

Example:

```sql
WITH x AS MATERIALIZED (SELECT * FROM big_table WHERE expensive_filter)
SELECT count() FROM x
UNION ALL
SELECT max(value) FROM x;
```

Expected: `x` is materialized once, both branches read the temporary table.
Observed: `big_table` is scanned twice with the expensive filter applied each
time; the CTE never materializes.

### Root cause

The duplication originates in `ApplyWithGlobalVisitor`
(`src/Interpreters/ApplyWithGlobalVisitor.cpp:26`). At AST level, it
propagates the `WITH` definitions from the first `ASTSelectQuery` of a
`UNION` into every other branch by `child->clone()`. This is correct and
necessary for non-materialized CTEs - each branch's scope needs its own
copy of the binding to resolve - but for materialized CTEs it produces
two AST nodes that look like independent CTE definitions to the
analyzer.

Then in the analyzer, `QueryAnalyzer::tryResolveIdentifierFromCTE`
(`src/Analyzer/Resolve/QueryAnalyzer.cpp:1293`) hits the cloned CTE node
independently in each branch's scope and runs:

```cpp
auto table_node = std::make_shared<TableNode>(full_name, cte_node, scope.context);
```

The `TableNode` constructor at `src/Analyzer/TableNode.cpp:80-93`
allocates a fresh `MaterializedCTE` shared_ptr per call. One logical CTE
becomes N `MaterializedCTE` objects (one per UNION branch), each with
its own random `temporary_table_name`, its own dummy storage, and -
after `finalizeMaterializedCTE` runs in each branch's resolution - its
own real `StorageMemory` and `TemporaryTableHolder`.

Two downstream consumers key off `MaterializedCTEPtr` identity:

1. `collectMaterializedCTEs` (`src/Planner/CollectMaterializedCTE.cpp:19`)
   keys its map by pointer, so distinct pointers stay distinct and each
   gets its own materialization plan + storage.
2. `inlineMaterializedCTEIfNeeded`
   (`src/Analyzer/inlineMaterializedCTEIfNeeded.cpp`) uses
   `reused_materialized_cte` - a set populated in
   `QueryAnalyzer::tryResolveIdentifierFromCTE` only on the
   *second-and-later* resolution that hits the same pointer
   (`src/Analyzer/Resolve/QueryAnalyzer.cpp:1302`). A UNION-clone CTE
   referenced once per branch is resolved once per pointer, never lands
   in the set, and so gets *inlined* by the visitor instead of staying
   materialized. This is the surface bug.

### Precedent

`CollectSetsVisitor` (`src/Planner/CollectSets.cpp:50`) is the
analogous deduplication pattern in the planner: it keys `PreparedSets`
entries by `getTreeHash({.ignore_cte = true})`. We follow the same
approach at the analyzer level for materialized CTEs.

## Goals

1. Materialized CTEs referenced from multiple UNION branches materialize
   exactly once and both branches read the shared temporary table.
2. No regression for the existing single-branch and in-branch
   repeat-reference cases (already handled by
   `tryResolveIdentifierFromCTE`'s pointer-identity reuse path).
3. No new public API; no new setting; no compat flag - the prior
   behavior was strictly buggy.
4. Distributed and parallel-replicas execution inherits the fix without
   special handling.

## Non-goals

- A smarter `ApplyWithGlobalVisitor` that does not clone for
  materialized CTEs. Considered and rejected: many code paths assume
  WITH definitions are physically present in each branch's AST; a
  scope-climb resolution would touch much more surface.

Note on scope: the dedup uses `.ignore_cte = true` on the subquery
hash, so it will *also* collapse two materialized CTEs that happen to
have identical bodies under different `cte_name`s (e.g.
`WITH x AS MATERIALIZED (SELECT 1), y AS MATERIALIZED (SELECT 1)
SELECT * FROM x, y`). This is a deliberate side-effect, parallel to
how `CollectSets` already deduplicates structurally-equal sets
regardless of source naming. Duplicate work is duplicate work; merging
them is a net win. Column aliases inside the body still keep CTEs
separate via `compare_aliases = true`, so `SELECT 1 AS a` vs
`SELECT 1 AS b` does not merge.

## Design

### Locus

Primary changes inside `src/Analyzer/inlineMaterializedCTEIfNeeded.cpp`.
Companion cleanup in `src/Analyzer/Resolve/QueryAnalyzer.{h,cpp}`.

`MaterializedCTE` is not changed (we considered storing nesting depth
on the struct but ruled it out: depth becomes stale once inlining
removes references).

#### Signature change

The public function loses the `reused_materialized_cte` out-parameter.
The set is now derivable from the deduped tree and is built locally:

```cpp
// before
void inlineMaterializedCTEIfNeeded(
    QueryTreeNodePtr & node,
    ReusedMaterializedCTEs & reused_materialized_cte,
    ContextPtr context);

// after
void inlineMaterializedCTEIfNeeded(
    QueryTreeNodePtr & node,
    ContextPtr context);
```

`ReusedMaterializedCTEs` (currently `std::unordered_set<MaterializedCTEPtr>`
typedef in the header) becomes a file-local detail and moves into
`inlineMaterializedCTEIfNeeded.cpp` along with
`InlineMaterializedCTEsVisitor`. Nothing outside the file uses the
type after the signature change.

#### Companion cleanup in `QueryAnalyzer`

The `std::unordered_set<MaterializedCTEPtr> reused_materialized_cte;`
member at `src/Analyzer/Resolve/QueryAnalyzer.h:306` is deleted. The
incremental `reused_materialized_cte.insert(table_node->getMaterializedCTE());`
at `src/Analyzer/Resolve/QueryAnalyzer.cpp:1302` is deleted. The call
site at line 274 becomes `inlineMaterializedCTEIfNeeded(node, context);`.

Rationale: before this change, the set was incrementally accumulated
during identifier resolution as a side-effect of `tryResolveIdentifierFromCTE`
encountering a CTE `TableNode` for the second time. That accumulation
was always tied to pointer identity; with dedup, identity is now
established post-resolution, so the only correct moment to count
references is *after* the dedup pass has finished. There is no longer
any value computed during resolution that the inline pass needs;
keeping the member and the line-1302 insert as a redundant
pre-population would just be dead work that risks future readers
treating it as load-bearing.

### Step A - `deduplicateMaterializedCTEs(node, context)` (file-local static)

1. Walk the tree with `traverseQueryTree(node, Everything{}, enter, leave)`
   mirroring the depth-tracking shape of `collectMaterializedCTEs`
   (`src/Planner/CollectMaterializedCTE.cpp:33-56`). Maintain a local
   `size_t depth = 0`; on entering a materialized-CTE `TableNode`,
   append `{table_node, depth}` to a `std::vector<Entry>` and `++depth`;
   on leaving, `--depth`. Depth is pass-local; not persisted anywhere.

2. Stable-sort entries by descending depth. This is the load-bearing
   ordering property: any subquery body that references an inner
   materialized CTE must have that inner CTE canonicalized first, so
   the inner's `TableNode::temporary_table_name` (which participates in
   `TableNode::updateTreeHashImpl`) is stable across cloned outer
   bodies.

3. Group by `entry.table_node->getMaterializedCTESubquery()->getTreeHash({.ignore_cte = true})`
   into `std::unordered_map<IQueryTreeNode::Hash, std::vector<size_t>>`.
   Defaults on the other two `CompareOptions` fields, so
   `compare_aliases = true` and `compare_types = true`. Within a hash
   bucket, verify with
   `subquery->isEqual(bucket_head_subquery, {.ignore_cte = true})`;
   inequality starts a new singleton bucket. Hash collisions never
   collapse semantically distinct CTEs.

   Why `ignore_cte = true`: the subquery body's outer `QueryNode` /
   `UnionNode` carries binding-context metadata (`is_cte`, `cte_name`,
   `is_materialized`). For the UNION-clone case both clones share the
   same `cte_name`, so the flag's value doesn't matter for this case.
   We use `true` to match the `CollectSets` precedent and to keep the
   hash a pure function of the body's content.

4. For every bucket with size >= 2, pick canonical = entry with
   lexicographically smallest `materialized_cte->temporary_table_name`
   (deterministic, traversal-order-independent - keeps EXPLAIN stable
   across multiple runs of the same query). For every non-canonical
   entry, mirror the handoff the analyzer already uses for the
   in-branch repeat-reference case
   (`src/Analyzer/Resolve/QueryAnalyzer.cpp:3135`):

   ```cpp
   table_node->materialized_cte = canonical.materialized_cte;
   table_node->setTemporaryTableName(canonical.materialized_cte->temporary_table_name);
   table_node->updateStorage(canonical.materialized_cte->storage, context);
   ```

   `updateStorage` (`src/Analyzer/TableNode.cpp:104`) resets `storage`,
   `storage_id`, `storage_lock`, and `storage_snapshot` consistently.
   `children[materialized_cte_subquery_index]` is left in place: it is
   structurally equal to the canonical's subquery by step 3, only the
   canonical's gets planned downstream, and keeping the local copy
   leaves EXPLAIN QUERY TREE output for that branch self-contained.

   Orphan lifecycle: the non-canonical `TableNode` releases its old
   `MaterializedCTE` shared_ptr (last strong ref); orphan destructs,
   orphan's `TemporaryTableHolder` destructs (unregistering its
   external-table row), orphan `StorageMemory` destructs. The back-ref
   from `StorageMemory` to `MaterializedCTE` is
   `MaterializedCTEWeakPtr` (`src/Storages/StorageMemory.h:129/149`)
   so no cycle. Clean handoff.

### Step B - `collectReusedMaterializedCTEs(node) -> ReusedMaterializedCTEs` (file-local static)

The set is built fresh from the deduped tree:

1. Walk the tree once; for each `TableNode` whose
   `getMaterializedCTE()` is non-null, increment a counter in a local
   `std::unordered_map<MaterializedCTEPtr, size_t>`.
2. Return a `ReusedMaterializedCTEs` containing every pointer with
   count >= 2.

After Step B, the set contains exactly the canonical pointers that are
referenced from >= 2 places in the deduped tree - including
cross-branch references that previously couldn't accumulate because
they hit distinct pointers. The set lives only for the remaining
duration of `inlineMaterializedCTEIfNeeded`; nothing outside the
function depends on it.

### Step C - existing `InlineMaterializedCTEsVisitor` (unchanged behavior)

Driven by the set returned from Step B. CTEs that survived dedup with
multiple references stay materialized; CTEs that genuinely have a
single use get inlined as today.

The existing `addExternalTable` loop in `inlineMaterializedCTEIfNeeded`
runs over the locally-built set; each `temporary_table_name` is
registered exactly once because the set contains only canonical
pointers.

## Edge cases

| Case | Behavior |
|------|----------|
| Recursive WITH containing MATERIALIZED | Already throws `UNSUPPORTED_METHOD` at `src/Analyzer/QueryTreeBuilder.cpp:354-356`. Dedup never runs. |
| Self-cycle (`WITH a AS MATERIALIZED (SELECT FROM b), b AS MATERIALIZED (SELECT FROM a)`) | Already throws `UNKNOWN_TABLE` during resolution (`tests/queries/0_stateless/04044_materialized_cte_cycle.sql`). Dedup never runs. |
| Subquery (`SelectQueryOptions::is_subquery == true`) | Dedup runs (analyzer always runs); Planner-side `collectMaterializedCTEs` early-returns unless `force_materialize_cte` is set (`src/Planner/CollectMaterializedCTE.cpp:25`). Dedup is a no-op cost here, no behavior change. |
| Distributed query | The initiator's analyzed tree is already deduped. The receiving node re-parses + re-analyzes from the AST/serialized form, runs `inlineMaterializedCTEIfNeeded` again, applies the same dedup. Outcome by construction is identical. |
| Parallel replicas | Same as distributed. Replica-side rebuild from query tree (e.g. `src/Storages/buildQueryTreeForShard.cpp`) operates on already-deduped state from the initiator and re-analyzes deduped state on each replica. |
| Two materialized CTEs with identical bodies, different `cte_name` | *Do* dedup. `ignore_cte = true` hides the outer-node binding metadata so the bucket sees one logical body. See "Non-goals" note. Test #5 pins this behavior. |
| Two materialized CTEs with same body but different column aliases (`SELECT 1 AS a` vs `SELECT 1 AS b`) | Stay separate. Inner aliases participate in the hash because `compare_aliases = true`. Test #2 pins this. |
| In-branch repeat reference (`SELECT FROM x JOIN x`) within a single SELECT | Already deduped by `tryResolveIdentifierFromCTE`'s pointer-reuse path (line 1302). Dedup is a no-op; the bucket sees only one entry per branch. |

## Testing

New test file: `tests/queries/0_stateless/04045_materialized_cte_union_dedup.sql`
(use `./tests/queries/0_stateless/add-test 04045_materialized_cte_union_dedup`
to allocate the prefix).

1. **Bug-reproducing UNION** - `WITH x AS MATERIALIZED (SELECT FROM big_table) SELECT count() FROM x UNION ALL SELECT max() FROM x`. Assert correctness of the result, and assert single materialization via `EXPLAIN PIPELINE` containing exactly one `MaterializingCTE` step. Optionally cross-check via `system.query_log` that `ProfileEvents['SelectedRows']` is consistent with reading the source once.

2. **Body equal, column names differ** - `WITH x AS MATERIALIZED (SELECT 1 AS a), y AS MATERIALIZED (SELECT 1 AS b) SELECT * FROM x UNION ALL SELECT * FROM y`. Different column aliases produce different `getTreeHash` (because `compare_aliases = true`). Assert `EXPLAIN PIPELINE` contains two `MaterializingCTE` steps.

3. **Body equal, types differ** - `WITH x AS MATERIALIZED (SELECT toUInt8(1) AS a), y AS MATERIALIZED (SELECT toUInt64(1) AS a) ...`. `compare_types = true` keeps them separate. Two `MaterializingCTE` steps.

4. **Nested materialized CTE under UNION** - `WITH inner AS MATERIALIZED (SELECT FROM t), outer AS MATERIALIZED (SELECT FROM inner) SELECT FROM outer UNION ALL SELECT FROM outer`. Exercises the deepest-first ordering. Assert one materialization per logical CTE (two total: one for `inner`, one for `outer`).

5. **Different `cte_name`, identical body** - `WITH x AS MATERIALIZED (SELECT FROM big_table), y AS MATERIALIZED (SELECT FROM big_table) SELECT FROM x UNION ALL SELECT FROM y`. Confirm the `.ignore_cte = true` choice: pins that the two CTEs *are* deduped (one `MaterializingCTE` step in `EXPLAIN PIPELINE`). This is the deliberate side-effect documented in non-goals.

6. **UNION + materialized CTE inside `WHERE x IN (...)`** - the shape from the existing `04042_materialized_cte_union.sql`, but with an actual `UNION ALL`. Exercises the `PreparedSets` path that was the original symptom on this branch.

7. **Three-branch UNION ALL, same CTE referenced from all three** - confirm bucket size > 2 works.

8. **Distributed read** - run the same UNION shape against a `Distributed` table over two shards; assert correctness and assert (via `system.query_log` on the remote shards) that each shard also materializes the CTE only once.

9. **Negative: CTE referenced once total (no UNION)** - assert it still inlines (the analyzer's existing behavior).

## Risks

- **Aliasing or storage-locking surprises in `updateStorage`.** The
  primitive is exercised by the analyzer's existing in-branch
  repeat-reference path, so we know it works under at least that
  shape. The dedup pass uses it more broadly. Tests #1, #7, and #8
  exercise the new shape (single UNION, three-branch UNION, and
  distributed read).
- **Hash collisions.** Defended against by structural `isEqual` check
  inside each bucket.
- **`reused_materialized_cte` rebuild misses a case.** Tests #2 and #3
  pin the negative: distinct CTEs that should *not* dedup still get
  materialized.
- **Surprise dedup of same-body different-name CTEs.** Pinned by test
  #5. If this side-effect is judged unacceptable later, flip
  `.ignore_cte` to `false` in both the hash call and the `isEqual`
  call (one-line change each); the rest of the design is unaffected.

## Out of scope (future work)

- Stronger sharing for the cross-shard / distributed case (the current
  design re-runs dedup independently on each node; if a shape ever
  needs *shared* materialization across nodes, it would be a separate
  effort layered on top).
- A general analyzer pass that deduplicates structurally-equal
  subqueries beyond materialized CTEs - e.g., the same scalar
  subquery written twice. Out of scope; would need its own design.
