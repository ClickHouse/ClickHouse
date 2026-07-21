# Cascades Optimizer — Architecture

## What Problem Does This Solve?

ClickHouse can execute a query on a single node using its standard pipeline: plan the
query, read from MergeTree, apply filters/expressions/joins/aggregations, return results.
But when data is spread across multiple nodes (shared storage like S3, or a multi-node
cluster), we need to decide HOW to distribute the work:

- Should a join shuffle both sides by the join key, or broadcast the smaller table to
  all nodes?
- Should an aggregation happen on each node first (partial agg) and then merge results,
  or gather all data to one node and aggregate there?
- Should a table scan be split across N nodes (parallel read), or should each node read
  the full table (replicated read for small dimension tables)?
- Where should exchanges (network data transfers between nodes) be placed?

These choices interact — the best join strategy depends on how the data will be
aggregated later, and the best aggregation strategy depends on the join output
distribution. A cost-based optimizer explores many combinations and picks the cheapest.

The Cascades framework uses **top-down, goal-directed search with memoization** — not
bottom-up dynamic programming like the `dpsize` join ordering algorithm. The optimizer
starts from the root goal (deliver results to the coordinator at `{1 node}`) and
recursively decomposes it into subgoals (e.g., "produce this join result at `{4 nodes,
shuffled by custkey}`"). Results are cached in the **memo** so each (group, properties)
pair is computed at most once. The framework also supports branch-and-bound pruning of
subtrees that cannot beat the current best; here it is currently disabled and the search
is bounded by a fail-closed task budget instead (see the search section below).

**Without Cascades**: ClickHouse's existing distributed query execution (via the
`Distributed` table engine or parallel replicas) uses fixed strategies — typically gather
all data to the coordinator, or use two-phase aggregation with a fixed distribution.
There's no cost-based comparison of broadcast vs shuffle vs local strategies.

**With Cascades**: The optimizer takes the logical query plan (already with a fixed join
order) and explores all valid physical execution strategies, estimates their cost, and
picks the cheapest. This is the same framework used by GPORCA (Greenplum), CockroachDB,
and StarRocks for distributed query planning.

## Where It Fits in the Query Processing Pipeline

```
SQL string
  ↓
Parser → AST
  ↓
Planner (PlannerCorrelatedSubqueries, PlannerJoinTree, etc.)
  → Logical query plan with JoinStepLogical, AggregatingStep, ExpressionStep, etc.
  ↓
Pre-Cascades optimizations:
  - optimizeJoin.cpp → fixes join order (dpsize/greedy) using column statistics
  - filterPushDown.cpp → pushes filters below joins
  - joinRuntimeFilter.cpp → adds bloom filter steps
  - other rule-based passes (convertOuterJoinToInner, etc.)
  ↓
Cascades optimizer (this code)                          ← NEW
  → Input: logical plan with fixed join order, no distribution info
  → Output: physical plan with exchanges, parallel reads, distribution strategies
  ↓
Post-Cascades: buildQueryPipeline
  → Converts plan steps to executable pipeline (Processors)
  → Exchanges become actual network communication
```

The Cascades optimizer is **only active** when both `enable_cascades_optimizer = 1` and
`make_distributed_plan = 1` are set. Without these settings, the existing pipeline
runs as before.

The feature fails closed. `make_distributed_plan` first rejects plan shapes the
distributed pipeline cannot execute correctly (`WITH TOTALS`, `ROLLUP`, `CUBE`, extremes,
`PASTE JOIN`) and distributed reads a worker cannot reproduce (pinned block-number
boundaries, `_part_index` / `_part_starting_offset`). When Cascades is active, a second
pre-check rejects reads that cannot be cloned, `LOCAL` joins, non-full sorts, and
in-order aggregation that relies on implicit input order; other non-shippable plans
(e.g. explicitly sorted in-order aggregation) fail fragment-serializability validation
before execution. During search, an expression whose input has no implementation for
the required properties is never recorded as a group's best, and a search that proves
the query has no distributable plan rejects it at the root. A plan that received
exchange steps but cannot be converted to distributed stages is rejected instead of
silently running the exchanges as no-ops locally, and a search that does not finish
within the task budget rejects the query instead of building a plan from a partial memo.

## Key Concepts

### The Memo

The memo is a data structure that compactly represents many alternative query plans
simultaneously. It consists of **groups**, each representing a set of logically equivalent
subexpressions.

For example, `customer ⋈ orders` and `orders ⋈ customer` (swapped) produce the same
logical result — they belong to the same group. Each group can have multiple **physical
expressions** with different execution strategies and properties:

```
Group #5 (customer ⋈ orders, ~5.6M rows):
  Logical:
    Join(customer, orders)
    Join(orders, customer)  [swapped by JoinCommutativity rule]
  Physical:
    Shuffle HashJoin by custkey  at {4 nodes}    cost: 2.93B
    Broadcast HashJoin           at {4 nodes}    cost: 6.2B
    Local HashJoin               at {1 node}     cost: 10.8B
```

The optimizer explores alternatives by applying rules that generate new expressions in
each group, then picks the cheapest physical expression that satisfies the parent's
required properties.

### Physical Properties

Each physical expression has **properties** describing what it produces:

- **Distribution**: How many nodes the data is spread across, whether it's replicated,
  and which columns it's partitioned by.
  - `{1 node}` — all data on the coordinator
  - `{4 nodes}` — data split across 4 nodes (any partitioning)
  - `{4 nodes, by custkey}` — data hash-partitioned by `custkey` across 4 nodes
  - `{4 nodes, replicated}` — full copy on every node

- **Sorting**: Whether the output is sorted and by which columns.

A parent step **requires** specific properties from its child. For example, a
`ShuffleHashJoin` on `custkey` requires both inputs to be distributed `{4 nodes, by custkey}`.
If the child doesn't naturally produce that distribution, an **enforcer** (exchange step)
is added to bridge the gap.

### Exchange Steps (New Step Types)

These are the network data transfer operators that Cascades adds to the plan:

| Step | What it does | Example use |
|---|---|---|
| `ShuffleExchange` | Hash-partition data by specified columns and send to N nodes | Redistribute for shuffle join |
| `GatherExchange` | Collect data from N nodes to 1 node | Gather partial aggregation results |
| `GatherExchange(sorted)` | Merge-sorted gather preserving sort order | Gather pre-sorted data |
| `BroadcastExchange` | Replicate full data from 1 node to all N nodes (each node gets a complete copy) | Small dimension table needed on every node |
| `ScatterExchange` | Distribute data from 1 node across N nodes; without partitioning columns, chunks go round-robin | 1-to-N redistribution, with or without specific partitioning |

### Read Strategies

| Strategy | What it does | When used |
|---|---|---|
| `ReadFromMergeTree` (local) | Standard single-node read | Default for `{1 node}` |
| `ParallelRead` | Split the table's parts across N nodes, each reads 1/N | Large tables, `{N nodes}` |
| `ReplicatedRead` | Each node reads the full table from shared storage (S3) | Small dimension tables — avoids network exchange overhead |

`ReplicatedRead` is a key optimization for shared-storage deployments. Instead of reading
a small table on one node and broadcasting it via `BroadcastExchange`, every node repeats
the same read. For a 25-row nation table, this eliminates network overhead entirely.
It assumes every worker sees the same complete table (shared storage); the rule does not
yet validate this per storage — a known limitation of the experimental version. There is
no size gate either: an oversized replicated read simply loses on cost.

### Enforcers

An enforcer is a physical expression that bridges a **property gap**. When a parent
requires `{1 node}` but the cheapest child produces `{4 nodes}`, the `DistributionEnforcer`
creates a `GatherExchange` that collects data from 4 nodes to 1.

Enforcers are **self-referential**: a `GatherExchange` in Group G has its input pointing
back to Group G itself, but with relaxed required properties (the input asks for
`{4 nodes}` while the output provides `{1 node}`). Left unchecked this can form a real
cycle, because a relaxed requirement acts as a wildcard: an empty required sort is
satisfied by any sorted expression, and empty required distribution columns are satisfied
by any keyed expression — including the enforcer itself or a sibling enforcer.

Two invariants keep enforcer self-reference acyclic:

- Every enforcer expression carries an **enforcer axis** (`Sorting` or `Distribution`).
  When resolving a self-referential enforcer input, an enforcer candidate that satisfies a
  wildcard axis only by over-providing on it is not eligible (a sorted enforcer cannot
  fill an empty-sort slot; a keyed exchange cannot fill an empty-columns slot). Productive
  compositions stay eligible: `Sort` feeding a sorted `GatherExchange`, a `GatherExchange`
  feeding a `Sort` or a `BroadcastExchange`.
- Plan extraction (`buildBestPlan`) tracks the single-input expressions on the current
  chain and resolves each input through the same eligibility rule, scanning the group's
  costed physical expressions as a fallback when the best-implementation cache holds only
  expressions already on the chain.

### Two-Phase Aggregation

This is similar to ClickHouse's existing `MergingAggregated` pattern but chosen by
cost. Given `GROUP BY n_name` with 25 distinct values:

- **Single-phase**: Gather 5.6M rows to 1 node, aggregate there → expensive network
- **Two-phase**: Aggregate on each of 4 nodes first (5.6M → 25 rows per node),
  gather 100 rows, merge → cheap network
- **Shuffle**: Shuffle 5.6M rows by `n_name` to N nodes, aggregate per-partition →
  expensive network, no reduction before shuffle

The optimizer creates all three as alternatives and compares their costs. Two-phase
wins when the GROUP BY has few distinct values (high aggregation factor).

## Architecture Details

### Task-Based Optimization

The optimizer uses a LIFO task stack. For each group, optimization proceeds through
four stages:

- **Stage 1 — Explore**: Fire transformation rules (`JoinCommutativity`,
  `TwoStageAggregation`, `TwoStageTopN`) to generate logically equivalent expressions.
- **Stage 2 — Implement**: Fire implementation rules (`HashJoinImplementation`,
  `AggregationImplementation`, `LocalReadImplementation`, `ParallelReadImplementation`,
  `ReplicatedReadImplementation`, `SortImplementation`, `DefaultImplementation`,
  `DistributionPassthrough`) to generate physical expressions with concrete properties.
- **Stage 3 — Enforce**: Apply enforcer rules (`DistributionEnforcer`, `SortingEnforcer`)
  to bridge property gaps. Uses a fixed-point loop for enforcer composition (e.g.,
  `SortingEnforcer` creates `Sort({N, sorted})`, then `DistributionEnforcer` creates
  `GatherExchange(sorted)` from it).
- **Stage 4 — Done**: Group is fully optimized for the requested properties.

The root group is optimized for `{1 node}` (the coordinator must return the final result).
Each child group is optimized for whatever properties its parent requires. The optimizer
works top-down, recursively optimizing each group. The search carries no per-subtree cost
budget: a best plan taken from a partially optimized sibling group is an upper bound, not
a lower bound, so a budget derived from it can prune plans that would become cheapest. The search is bounded by the task budget instead, which
fails closed: if optimization does not finish within the budget, the query is rejected
with a clear error (naming the remaining task) rather than built from a partial memo.

The four stages are driven by six concrete task types on the stack: `OptimizeGroupTask`
and `ExploreGroupTask` drive a whole group; `ExploreExpressionTask` and
`OptimizeExpressionTask` collect the transformation resp. implementation rules that match
one expression; `ApplyRuleTask` runs a chosen rule; and `OptimizeInputsTask` walks an
expression's inputs and costs it once they are all optimized.

Each rule carries a static **promise** (a priority). The rules applicable to an
expression are sorted by promise and pushed on the LIFO stack, so the highest-promise
rule runs first. Promise does not change which plans are considered — every applicable
rule is eventually applied — but it orders their generation, which breaks ties among
equal-cost alternatives (the first-found best is kept and a later equal-cost candidate is
pruned). Enforcer rules are scheduled by the Stage-3 fixed-point loop, so their promise
is not consulted.

**Key files**: `Task.h/cpp`, `OptimizerContext.h/cpp`, `Optimizer.cpp`

### Enforcer Scheduling

Stage 3 is gated by `!isEnforcedFor(required_properties)`, ensuring enforcers run exactly
once per (group, properties) pair even when a satisfying implementation already exists.
This lets enforcer-created plans (e.g., `GatherExchange` on a distributed subtree)
compete on cost with direct implementations.

The fixed-point loop within Stage 3 handles **enforcer composition**: iterates over
newly-added physical expressions until no new enforcers are produced. The dedup key
includes sorting state so that `DistributionEnforcer` fires separately for sorted vs
unsorted source expressions.

**Pruning** at the top of `OptimizeGroupTask` returns early when the group is explored,
optimized, and enforced for the requested properties and already has a satisfying best
implementation, preventing re-entry loops from self-referential enforcers.

### Implementation Strategies

A physical expression is a query plan step paired with an **implementation strategy**
(`ImplementationStrategyPtr` on `GroupExpression`). The strategy names the physical
algorithm chosen for that step and owns its per-operator cost. Logical expressions and
`DefaultImplementation` passthroughs carry no strategy (`strategy == nullptr`).

Strategies are grouped by operator family — `IJoinStrategy`, `IAggregationStrategy`,
`IReadStrategy` — and each concrete strategy implements `estimateOperatorCost` in
`Cost.cpp` (kept there so every cost formula lives in one file):

- Joins: `LocalJoinStrategy`, `BroadcastJoinStrategy`, `ShuffleJoinStrategy`
- Aggregation: `LocalAggregationStrategy`, `ShuffleAggregationStrategy`, `PartialAggregationStrategy`
- Reads: `ParallelReadStrategy`, `ReplicatedReadStrategy`

`estimateOperatorCost` dispatches on the family with `dynamic_cast`; a logical operator
still without a strategy is priced as its cheapest reasonable default (a non-broadcast
hash join, or local aggregation) so a partially implemented plan still gets a finite cost.

Two strategies are markers with no cost function of their own:

- `PartialTopNStrategy` tags the per-shard bounded sort that `TwoStageTopN` creates, so
  the transformation does not split it again.
- `ReplicatedSubplanStrategy` marks a step run identically on every node over replicated
  inputs; it satisfies `{node_count=N, is_replicated=true}` without a `BroadcastExchange`.
  Replicated expressions get parallelism 1.0, so the default per-step formulas already
  charge the full work each node repeats.

**Key files**: `ImplementationStrategy.h`, `Cost.cpp`

### Cost Model

Three-component cost with `work`, `network`, and `sequential` dimensions,
combined via configurable weights:

```
total_cost = work * work_weight + network * network_weight + sequential * sequential_weight
```

- `work`: rows or bytes processed, divided by parallelism (I/O + CPU combined)
- `network`: bytes transferred between nodes
- `sequential`: single-threaded phases (hash table builds, merge cursors)

In addition, every exchange adds a fixed `exchange_fixed_overhead` to `sequential`
(connection setup and metadata), which keeps tiny-table exchanges from looking free.
A `BroadcastExchange` charges its network transfer once per receiving node, and a
partial top-N is charged for scanning its whole input while its sorted gather carries
up to `limit * node_count` rows.

With a high `sequential_weight`, the optimizer prefers plans that minimize
single-node bottlenecks (e.g., shuffle over broadcast for large hash tables).

The whole cost configuration is overridable at query time via one JSON parameter —
the three weights, the per-exchange overhead, and the calibration constants
(`expression_cost_per_row`, `hash_table_build_factor`, `unknown_leaf_cost`,
`funnel_sequential_cost_per_row`, `merge_sequential_cost_per_row`):
```sql
SET param__internal_cascades_cost_config = '{"work_weight":1,"network_weight":1,"sequential_weight":1000}';
```

`cpu_weight` is also accepted as a legacy alias for `work_weight` when `work_weight`
is absent. All values must be finite and non-negative (zero is allowed to ignore a
dimension); an
invalid config rejects the query instead of silently falling back to defaults, and an
infinite cost component stays infinite under any weights, so an impossible plan can
never win by a zero weight.

The cluster size is derived automatically from the worker configuration
(`stateless_worker_client.cluster`, or a single `stateless_worker_client.host`) — the
same source `TaskToHostMap` uses.  A query parameter overrides it for testing (it also
caps the host list used for execution):
```sql
SET param__internal_cascades_cluster_node_count = 20;
```
When the node count cannot be determined (no worker configuration and no parameter) the
query is rejected rather than planned for a single node, which would silently skip every
distributed alternative.

The optimizer explores only two parallelism levels: `{1 node}` (local) and `{N nodes}`
(full cluster).  Intermediate counts are not explored because they were never chosen
on TPC-H and would multiply the search space. The rule-based planner's
`distributed_plan_default_shuffle_join_bucket_count`,
`distributed_plan_default_reader_bucket_count`, and
`distributed_plan_max_rows_to_broadcast` heuristics are not used: fan-out and
broadcast-vs-shuffle are decided by estimated cost.

The per-query environment (cluster size, cost configuration, and the query settings the
rules honor) is fixed before the search starts and lives on the memo
(`OptimizationEnvironment`). The rules honor `distributed_aggregation_memory_efficient`
and `distributed_plan_force_shuffle_aggregation` for aggregation, and
`exact_rows_before_limit` disables the two-stage top-N (its internal per-shard cap would
break the exact `rows_before_limit_at_least` accounting). The query's sort settings (size
limits, spill thresholds) are taken from the query context when the search starts and
reused when `SortingEnforcer` creates a new sort.

Every costed expression is traced at test log level with its `work`/`network`/`sequential`
breakdown, and several rules log the reason for non-obvious refusals (an unsplittable read,
an untranslatable distribution column), so most plan choices can be reconstructed from the
logs of one `EXPLAIN` run.

**Key files**: `Cost.h/cpp`

### `best_implementations` Index

Best implementations per group are stored in an
`unordered_map<UInt64, vector<GroupExpressionPtr>>` keyed by distribution shape
`(node_count, is_replicated)`. This gives O(1) bucket lookup followed by a linear scan
of the non-dominated alternatives for that distribution shape.

An expression whose input has no implementation for its required properties is never
recorded (see `ExpressionCost::buildable`), so plan extraction cannot walk into a
missing subtree.

A Pareto frontier is maintained: when a new implementation is added, dominated entries
(same or broader properties at higher cost) are removed. One exception protects plan
extraction: an enforcer never suppresses or evicts a non-enforcer implementation with
strictly weaker properties, so the acyclic base alternative stays reachable even when a
cheaper enforcer covers its requirement.

### Rules

The lists below use class names; `getName` log names may omit the `Implementation` suffix.

**Transformation rules** (generate logically equivalent expressions):
- `JoinCommutativity` — swaps join sides (left ↔ right) for joins where the swap is
  semantics-preserving: `INNER ALL`, `CROSS`, and `SEMI`/`ANY`/`ANTI` strictness;
  never `ASOF` or `INNER ANY`
- `TwoStageAggregation` — splits aggregation into partial + merge
- `TwoStageTopN` — splits a top-N sort into a per-node bounded sort, a sorted-merge
  gather, and a coordinator limit

**Implementation rules** (generate physical expressions with properties):
- `HashJoinImplementation` — creates local joins, broadcast joins (skipped for join
  kinds where a replicated build side would duplicate output rows), full-key shuffle
  joins, and single-key shuffle alternatives for multi-key equi-joins. A broadcast
  join keeps every left row on its node, so when the parent-required distribution
  columns map to equi-join keys or to surviving left-side columns, a keyed broadcast
  variant requires the left input partitioned by them and advertises them on the
  output instead of forcing a shuffle above the join.
- `AggregationImplementation` — creates local and shuffle aggregation (including
  single-key alternatives for multi-key `GROUP BY`) and implements the partial
  aggregations; shuffle is not created for global aggregation, grouping sets, overflow
  rows, or `max_rows_to_group_by`
- `LocalReadImplementation` — single-node read
- `ParallelReadImplementation` — parallel N-way read across nodes
- `ReplicatedReadImplementation` — full table read on each node (shared storage)
- `SortImplementation` — bounded sort at one node, or per node for the top-N partial
- `DefaultImplementation` — wraps otherwise-unhandled steps at `{1 node}`; operators
  with dedicated rules above are excluded
- `DistributionPassthrough` — propagates distribution through stateless per-row steps
  (`ExpressionStep`, `FilterStep`, `BuildRuntimeFilterStep`), translating distribution
  and sort columns through the step's `ActionsDAG` and creating sorted passthrough
  variants; expressions with per-block or non-deterministic functions stay single-node

**Enforcer rules** (bridge property gaps):
- `DistributionEnforcer` — adds `GatherExchange`, `BroadcastExchange`,
  `ShuffleExchange`, `ScatterExchange` (keyed, or column-less round-robin for a
  multi-node requirement with no partitioning constraint). Produces both regular
  and sorted-merge gather variants.
- `SortingEnforcer` — adds `SortingStep` with required sort description.

**Key files**: `Rules/*.cpp`

### Statistics

Statistics are derived on-demand during rule application and cached on groups. Each
group has `estimated_row_count` (plus a proven `max_row_count`, per-column NDVs, and a
byte width) used for cost estimation. Read groups — including an initial filter directly
over a read — are prepopulated from index analysis, column statistics, or test hints;
join and aggregation statistics are derived by `StatisticsDerivation.cpp`. Join estimates
are clamped to the semantics of the join kind and strictness (an outer join keeps its
preserved side, semi/anti/any joins cannot exceed it, a paste join is position-wise),
and the join row width comes from the actual output header.

**Key files**: `Statistics.h/cpp`, `StatisticsDerivation.cpp`

---

## Comparison with Classic Cascades and Other Systems

### What aligns well

| Aspect | Status | Notes |
|--------|--------|-------|
| Memo structure | Aligned | Standard groups with logical/physical expressions |
| Property model | Aligned (extended) | Distribution + sorting; column equivalence sets extend basic Cascades |
| Rule categorization | Aligned | Transformation / implementation / enforcer separation |
| Cost model | Aligned | 3-D (work, network, sequential) with configurable weights |
| Branch-and-bound | Not yet | No per-subtree cost bounds (unsound with partially optimized siblings); the fail-closed task budget bounds the search |
| Enforcer scheduling | Aligned | `isEnforcedFor` gate + fixed-point composition |
| Sorting as property | Aligned | Stripped from memo, enforced via composition |
| `best_implementations` lookup | Aligned | Indexed by distribution shape |
| Property hashing | Aligned | Structural `ExpressionPropertiesHash` + `boost::hash_combine` for fingerprints |

### Differences from classic Cascades

**Join ordering outside Cascades**: The most significant difference. In classic Cascades
(Columbia, GPORCA, CockroachDB, StarRocks), join ordering IS the Cascades optimization.
ClickHouse uses a two-phase architecture: `optimizeJoin.cpp` fixes the join order first,
then Cascades optimizes only the distribution strategy. This means join order is optimized
for local cost, not distributed cost.

Gretscher & Dittrich (PVLDB 2025, "How to Optimize SQL Queries? A Comparison Between
Split, Holistic, and Hybrid Approaches") name these two designs SPLIT (join order then
physical, as here) and HOLISTIC (fused, as classic Cascades), and measure a hybrid TOP-K
in between: enumerate the k best join orders, optimize each physically, keep the cheapest.
Their result is that SPLIT loses the global optimum only when join order interacts with
physical properties (sort order, fused operators, and here distribution), and that TOP-K
with k around 5-10 recovers most of the HOLISTIC benefit at a fraction of the cost.
Feeding several top orders into the memo (Future Work item 7) is that hybrid; the current
optimizer is the k = 1 case, kept deliberately because larger k multiplies the search space.

**Structural property hashing**: `isOptimizedFor`, `isEnforcedFor`, and physical
expression deduplication use `ExpressionPropertiesHash` for O(1) lookup.  Expression
fingerprints (used to deduplicate physical expressions within a group) are computed
as `size_t` hashes combining step description, strategy, properties, and input group
IDs via `boost::hash_combine`.

**Statistics dependency**: `estimateReadRowsCount` calls back into pre-Cascades code,
creating a dependency cycle.

**Task budget instead of pruning**: A budget of 100,000 tasks bounds the search (a query
parameter can lower it for tests, never raise it). Exhausting the budget rejects the
query instead of building a plan from a partial memo. Classic Cascades relies on
branch-and-bound pruning as the primary bound.

---

## Known Limitations and Future Work

### Performance

1. **Decouple statistics** from pre-Cascades code.
2. **Convergence detection and sound branch-and-bound pruning** instead of relying on
   the fail-closed task budget alone.

### Cost Model

3. **Filter selectivity**: a standalone `FilterStep` (above a join or expression) is
   currently modeled as selectivity 1; only filters fused into reads are estimated.
4. **`arrayJoin` fan-out**: an `ExpressionStep` with `arrayJoin` grows the row count,
   which is not estimated yet.

### Optimizer Features

5. **`ReplicatedRead` validation and gating**: the rule assumes every worker sees the
   same complete table (shared storage) and has no hard size gate; an oversized
   replicated read loses only on cost.
6. **Runtime bloom filter placement in Cascades**: the pre-Cascades pass may add
   `BuildRuntimeFilterStep` and Cascades passes it through, but Cascades does not
   create or cost runtime-filter alternatives. The single biggest gap vs StarRocks.
7. **Join ordering in Cascades**: the pre-Cascades join orderer already offers `dphyp`
   (inner joins) alongside `dpsub`, `dpsize`, and `greedy`; in an algorithm chain such as
   `dphyp,greedy`, unsupported cases fall through to the next algorithm. Cascades still
   receives a single fixed order; feeding the top k orders into the memo (the hybrid TOP-K
   of Gretscher & Dittrich, PVLDB 2025) is future work.
8. **Window function distribution**: `WindowStep` currently goes through
   `DefaultImplementation` at `{1 node}`. Needs a `WindowImplementation` rule
   that sets distribution by PARTITION BY key.
9. **CTE / common subplan sharing**: Detect `CommonSubplanReferenceStep` and
   map to existing groups instead of cloning.
10. **Dependent group-by key elimination**: Remove redundant GROUP BY columns using
   functional dependencies from MergeTree keys.

---

## Worked Example

This section traces the optimizer through a concrete query to show how all the
pieces work together.

### Query

```sql
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_cascades_cost_config = '{"work_weight":1,"exchange_fixed_overhead":100,"network_weight":1,"sequential_weight":1000}';

SELECT
    n_name,
    SUM(o_totalprice) AS total_revenue
FROM orders
JOIN customer ON o_custkey = c_custkey
JOIN nation ON c_nationkey = n_nationkey
WHERE o_orderdate >= '1995-01-01' AND o_orderdate < '1995-04-01'
GROUP BY n_name
ORDER BY total_revenue DESC
SETTINGS
    enable_cascades_optimizer = 1,
    make_distributed_plan = 1,
    send_logs_level = 'test',
    enable_join_runtime_filters = 0,
    enable_parallel_replicas = 0;
```

Database: `tpch100_auto_statistics` (SF100).
Tables: orders 150M rows, customer 15M rows, nation 25 rows.
After date filter: orders ~5.6M rows.

This query exercises: shuffle join (customer ⋈ orders), broadcast join with
`ReplicatedRead` (nation), two-phase aggregation, and sorting enforcement.

### Input from join order optimizer

Before Cascades runs, `optimizeJoin.cpp` fixes the join order using column statistics:

```
Expression (Project names)
  Sorting (ORDER BY total_revenue DESC)
    Expression
      Aggregating (GROUP BY n_name)
        Expression
          JoinLogical (rows: ~5.6M)                    -- (customer ⋈ orders) ⋈ nation
            JoinLogical (rows: ~5.6M)                  -- customer ⋈ orders
              Expression
                ReadFromMergeTree (customer)            -- 15M rows
              Expression (WHERE o_orderdate range)
                ReadFromMergeTree (orders)              -- 5.6M rows after filter
            Expression
              ReadFromMergeTree (nation)                -- 25 rows
```

No distribution properties, no exchanges, no costs. `SortingStep` is still a logical
step — Cascades will strip it into a property.

### Memo construction

Each operator becomes a group. `SortingStep` is stripped — the sort description
`[total_revenue DESC]` becomes a required property on Group #1's input link.
`TwoStageAggregation` transformation creates an additional Group #12.

```
Group #0:  Expression (Project names)         -- root, required: {1 node}
Group #1:  Expression (before ORDER BY)       -- required: {1 node, sorted [revenue DESC]}
Group #2:  Aggregating (GROUP BY n_name)
Group #3:  Expression (before GROUP BY)
Group #4:  Join (join#5 ⋈ nation)             -- inputs: #5, #10
Group #5:  Join (customer ⋈ orders)           -- inputs: #6, #8
Group #6:  Expression -> customer
Group #7:  ReadFromMergeTree (customer)        -- 15M rows
Group #8:  Expression -> orders
Group #9:  ReadFromMergeTree (orders)          -- 5.6M rows
Group #10: Expression -> nation
Group #11: ReadFromMergeTree (nation)          -- 25 rows
Group #12: Aggregating (Partial)              -- from TwoStageAggregation rule
```

### Implementation rules: read strategies

For **nation** (Group #11, 25 rows):

| Physical Expression | Distribution | Cost |
|---|---|---|
| `ParallelRead` | `{4 nodes}` | 425 |
| `ReadFromMergeTree` (local) | `{1 node}` | 1,700 |
| `ReplicatedRead` | `{4 nodes, replicated}` | 1,700 |

`ReplicatedRead` costs the same as local read (each node reads the full 25-row table
from shared storage) — cheaper than `ShuffleExchange` (cost 102,125) where exchange
overhead dominates for tiny tables.

For **orders** (Group #9, 5.6M rows after date filter):

| Physical Expression | Distribution | Cost |
|---|---|---|
| `ParallelRead` | `{4 nodes}` | 19,555,900 |
| `ReadFromMergeTree` (local) | `{1 node}` | 78,223,600 |
| `ShuffleExchange(o_custkey)` | `{4 nodes, by o_custkey}` | 97,879,500 |
| `ReplicatedRead` | `{4 nodes, replicated}` | 78,223,600 |

`ParallelRead` at 4 nodes is cheapest (1/4 of data per node). `ShuffleExchange`
is added by `DistributionEnforcer` when a downstream join requires a specific
partitioning. `ReplicatedRead` is expensive for large tables (each node reads
all 5.6M rows).

### Implementation rules: join strategies

For **customer ⋈ orders** (Group #5, table abbreviated — losing broadcast alternatives
for both join orders are omitted):

| Strategy | Distribution | Subtree Cost | Best? |
|---|---|---|---|
| **Shuffle HashJoin** (by custkey) | `{4 nodes}` | **3,069,696,580** | **Yes** |
| Local HashJoin | `{1 node}` | 11,485,091,920 | |

Shuffle wins — both tables are large. Broadcasting either side would replicate millions
of rows. Shuffling by `custkey` sends each row to exactly one node.

For **join#5 result ⋈ nation** (Group #4):

| Strategy | Distribution | Subtree Cost | Best? |
|---|---|---|---|
| **Broadcast HashJoin** (nation replicated) | `{4 nodes}` | **3,072,555,585** | **Yes** |
| Shuffle HashJoin (by nationkey) | `{4 nodes}` | 3,140,051,400 | |
| Local HashJoin | `{1 node}` | 8,759,587,117 | |

Broadcast wins — nation has 25 rows. The build side uses `ReplicatedRead` (cost 1,700)
instead of `BroadcastExchange` — each node reads the full nation table from shared
storage, avoiding network overhead entirely.

### Implementation rules: aggregation

`TwoStageAggregation` transformation created Group #12 (`Aggregating Partial`).

| Strategy | Distribution | Subtree Cost | Best? |
|---|---|---|---|
| **MergingAggregated -> GatherExchange -> Partial** | `{1 node}` | **3,074,253,216** | **Yes** |
| Aggregating (Shuffle, by n_name) | `{4 nodes}` | 3,478,204,166 | |

Two-phase wins — GROUP BY `n_name` has only 25 distinct values. Partial aggregation
on 4 nodes reduces 5.6M rows to ~25 per node (100 rows cross the network in total; the
`EXPLAIN` estimate shows the logical group result of 25), then `GatherExchange` feeds one
node for `MergingAggregated`. The shuffle alternative would send 5.6M rows through
`ShuffleExchange` before aggregating.

### Enforcer composition: sorting

Group #1 requires `{1 node, sorted by [total_revenue DESC]}`. The fixed-point loop
composes sorting with distribution enforcers:

1. `SortingEnforcer` creates `Sort({1 node, sorted})` from unsorted `{1 node}` expression
2. `SortingEnforcer` creates `Sort({4 nodes, sorted})` from unsorted `{4 nodes}` expression
3. `DistributionEnforcer` creates `GatherExchange(sorted)` from `Sort({4 nodes, sorted})`

Two competing plans for `{1 node, sorted}`:
- **Strategy A**: `Sort({1 node})` — sort locally after gather. Cost: 3,074,278,332
- **Strategy B**: `GatherExchange(sorted) -> Sort({4 nodes})` — sort per node, merge-gather

Strategy A wins — only 25 rows to sort after two-phase aggregation.

### Final plan

`EXPLAIN pretty = 1, estimates = 1` shows the chosen plan with the per-step row estimate and
the cumulative subtree cost, so the numbers from the sections above are visible in place:

```
Expression (Project names) (rows: <unknown>, cost: <unknown>)
└──Expression ((Before ORDER BY + Projection)) (rows: ~25.0, cost: 3074278334.9)
   └──Sorting (rows: ~25.0, cost: 3074278332.4)                  -- on 1 node, 25 rows
      └──MergingAggregated (rows: ~25.0, cost: 3074253216.3)     -- final merge on 1 node
         └──GatherExchange (rows: ~25.0, cost: 3074226166.3)     -- collect ~100 partial results
            └──Aggregating (rows: ~25.0, cost: 3074099166.3)     -- partial, 5.6M -> 25 rows per node
               └──Expression ((Before GROUP BY + )) (rows: ~5611180.6, cost: 3072695864.9)
                  └──JoinLogical (Broadcast HashJoin ) (rows: ~5611180.6, cost: 3072555585.4)
                     ├──JoinLogical (Shuffle HashJoin ) (rows: ~5611180.6, cost: 3069696580.1)
                     │  ├──Expression (rows: ~15000000.0, cost: 150475000.0)
                     │  │  └──ShuffleExchange (rows: ~15000000.0, cost: 150100000.0)   -- by custkey
                     │  │     └──ReadFromMergeTree (ParallelRead customer) (rows: ~15000000.0, cost: 30000000.0)
                     │  └──Expression (WHERE) (rows: ~5587400.0, cost: 98019185.0)
                     │     └──ShuffleExchange (rows: ~5587400.0, cost: 97879500.0)     -- by custkey
                     │        └──ReadFromMergeTree (ParallelRead orders) (rows: ~5587400.0, cost: 19555900.0)
                     └──Expression (rows: ~25.0, cost: 1702.5)
                        └──ReadFromMergeTree (ReplicatedRead nation) (rows: ~25.0, cost: 1700.0)
```

Total subtree cost: **3,074,278,335**. Optimization: **13 ms**, **444 tasks**.

### Summary of decisions

| Decision | Choice | Why |
|---|---|---|
| customer ⋈ orders | **Shuffle** by `custkey` | Both large (15M, 5.6M); broadcast would replicate millions |
| result ⋈ nation | **Broadcast** with `ReplicatedRead` | 25 rows; shared storage avoids network |
| Aggregation | **Two-phase** (partial -> gather -> merge) | 25 groups; partial agg reduces 5.6M to ~25 per node |
| Sorting | **Local** (sort after gather) | 25 rows after merge agg; trivial |
| Orders read | **ParallelRead** (4 nodes) | Splits 5.6M across 4 nodes |
| Nation read | **ReplicatedRead** | 25 rows; cheaper than exchange overhead |
