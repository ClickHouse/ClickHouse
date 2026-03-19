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
pair is computed at most once. Branch-and-bound pruning skips subtrees that can't beat
the current best, avoiding exhaustive exploration.

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

## Key Concepts

### The Memo

The memo is a data structure that compactly represents many alternative query plans
simultaneously. It consists of **groups**, each representing a set of logically equivalent
subexpressions.

For example, `customer ⋈ orders` and `orders ⋈ customer` (swapped) produce the same
logical result — they belong to the same group. Each group can have multiple **physical
expressions** with different execution strategies and properties:

```
Group #5 (customer ⋈ orders, ~5.3M rows):
  Logical:
    Join(customer, orders)
    Join(orders, customer)  [swapped by JoinCommutativity rule]
  Physical:
    Shuffle HashJoin by custkey  at {4 nodes}    cost: 2.93B
    Shuffle HashJoin by custkey  at {2 nodes}    cost: 5.57B
    Local HashJoin               at {1 node}     cost: 10.8B
    Broadcast HashJoin           at {4 nodes}    cost: (pruned)
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
| `ScatterExchange` | Distribute data from 1 node across N nodes (each node gets a disjoint slice) | 1-to-N redistribution without specific partitioning |

### Read Strategies

| Strategy | What it does | When used |
|---|---|---|
| `ReadFromMergeTree` (local) | Standard single-node read | Default for `{1 node}` |
| `ParallelRead` | Split the table's parts across N nodes, each reads 1/N | Large tables, `{N nodes}` |
| `ReplicatedRead` | Each node reads the full table from shared storage (S3) | Small dimension tables — avoids network exchange overhead |

`ReplicatedRead` is a key optimization for shared-storage deployments. Instead of reading
a small table on one node and broadcasting it via `BroadcastExchange`, every node reads
it directly from S3. For a 25-row nation table, this eliminates network overhead entirely.

### Enforcers

An enforcer is a physical expression that bridges a **property gap**. When a parent
requires `{1 node}` but the cheapest child produces `{4 nodes}`, the `DistributionEnforcer`
creates a `GatherExchange` that collects data from 4 nodes to 1.

Enforcers are **self-referential**: a `GatherExchange` in Group G has its input pointing
back to Group G itself, but with different required properties. This is not a cycle —
the input asks for `{4 nodes}` while the output provides `{1 node}`. The optimizer
resolves the `{4 nodes}` requirement by finding the cheapest `{4 nodes}` expression
already in the group (e.g., a `ParallelRead` or a `ShuffleHashJoin`).

### Two-Phase Aggregation

This is similar to ClickHouse's existing `MergingAggregated` pattern but chosen by
cost. Given `GROUP BY n_name` with 25 distinct values:

- **Single-phase**: Gather 5.3M rows to 1 node, aggregate there → expensive network
- **Two-phase**: Aggregate on each of 4 nodes first (5.3M → 25 rows per node),
  gather 100 rows, merge → cheap network
- **Shuffle**: Shuffle 5.3M rows by `n_name` to N nodes, aggregate per-partition →
  expensive network, no reduction before shuffle

The optimizer creates all three as alternatives and compares their costs. Two-phase
wins when the GROUP BY has few distinct values (high aggregation factor).

## Architecture Details

### Task-Based Optimization

The optimizer uses a LIFO task stack. For each group, optimization proceeds through
four stages:

- **Stage 1 — Explore**: Fire transformation rules (`JoinCommutativity`,
  `TwoPhaseAggregation`) to generate logically equivalent expressions.
- **Stage 2 — Implement**: Fire implementation rules (`HashJoinImplementation`,
  `AggregationImplementation`, `ParallelReadImplementation`, `DefaultImplementation`,
  `DistributionPassthrough`) to generate physical expressions with concrete properties.
- **Stage 3 — Enforce**: Apply enforcer rules (`DistributionEnforcer`, `SortingEnforcer`)
  to bridge property gaps. Uses a fixed-point loop for enforcer composition (e.g.,
  `SortingEnforcer` creates `Sort({N, sorted})`, then `DistributionEnforcer` creates
  `GatherExchange(sorted)` from it).
- **Stage 4 — Done**: Group is fully optimized for the requested properties.

The root group is optimized for `{1 node}` (the coordinator must return the final result).
Each child group is optimized for whatever properties its parent requires. The optimizer
works top-down, recursively optimizing each group, with **branch-and-bound pruning**:
if a partial plan already exceeds the cost of the best known plan, the subtree is skipped.

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

**Pruning** at the top of `OptimizeGroupTask` uses `isExplored() && isOptimizedFor()`
to prevent re-entry loops from self-referential enforcers.

### Cost Model

Multi-component cost with `cpu`, `memory`, `network`, `io`, and `sequential` dimensions,
combined via configurable weights:

```
total_cost = cpu * cpu_weight + memory * memory_weight + network * network_weight
           + io * io_weight + sequential * sequential_weight
```

The `sequential` component captures operations that cannot be parallelized (e.g., final
merge on coordinator). With a high `sequential_weight`, the optimizer prefers plans that
minimize single-node bottlenecks.

Cost weights are configurable at query time via:
```sql
SET param__internal_cascades_cost_config = '{"cpu_weight":1,"network_weight":1,"sequential_weight":1000,...}';
```

The cluster size (number of nodes) is set via:
```sql
SET param__internal_cascades_cluster_node_count = 20;
```

**Key files**: `Cost.h/cpp`

### `best_implementations` Index

Best implementations per group are stored in an
`unordered_map<UInt64, vector<GroupExpressionPtr>>` keyed by distribution shape
`(node_count, is_replicated)`. This gives O(1) bucket lookup followed by a small linear
scan (typically 2-5 entries per bucket).

A Pareto frontier is maintained: when a new implementation is added, dominated entries
(same or broader properties at higher cost) are removed.

### Rules

**Transformation rules** (generate logically equivalent expressions):
- `JoinCommutativity` — swaps join sides (left ↔ right)
- `TwoPhaseAggregation` — splits aggregation into partial + merge

**Implementation rules** (generate physical expressions with properties):
- `HashJoinImplementation` — creates local, broadcast, and shuffle join strategies
  at each candidate node count
- `AggregationImplementation` — creates aggregation at required distribution
- `ParallelReadImplementation` — parallel N-way read from MergeTree
- `ReplicatedReadImplementation` — full table read on each node (shared storage)
- `DefaultImplementation` — wraps any step at `{1 node}` as fallback
- `DistributionPassthrough` — propagates distribution through stateless per-row steps
  (`ExpressionStep`, `FilterStep`, `BuildRuntimeFilterStep`)

**Enforcer rules** (bridge property gaps):
- `DistributionEnforcer` — adds `GatherExchange`, `BroadcastExchange`,
  `ShuffleExchange`, `ScatterExchange`. Produces both regular and sorted-merge
  gather variants.
- `SortingEnforcer` — adds `SortingStep` with required sort description.

**Key files**: `Rules/*.cpp`

### Statistics

Statistics are derived on-demand during rule application and cached on groups. Each
group has `estimated_row_count` used for cost estimation. Statistics for leaf groups
(table scans) come from MergeTree column statistics; join/aggregation statistics are
derived by `StatisticsDerivation.cpp`.

**Key files**: `Statistics.h/cpp`, `StatisticsDerivation.cpp`

---

## Comparison with Classic Cascades and Other Systems

### What aligns well

| Aspect | Status | Notes |
|--------|--------|-------|
| Memo structure | Aligned | Standard groups with logical/physical expressions |
| Property model | Aligned+ | Distribution + sorting; column equivalence sets extend basic Cascades |
| Rule categorization | Aligned | Transformation / implementation / enforcer separation |
| Cost model | Better | Multi-component vs single scalar; configurable weights |
| Branch-and-bound | Aligned | Cost limits propagated through tasks |
| Enforcer scheduling | Aligned | `isEnforcedFor` gate + fixed-point composition |
| Sorting as property | Aligned | Stripped from memo, enforced via composition |
| `best_implementations` lookup | Aligned | Indexed by distribution shape |

### Differences from classic Cascades

**Join ordering outside Cascades**: The most significant difference. In classic Cascades
(Columbia, GPORCA, CockroachDB, StarRocks), join ordering IS the Cascades optimization.
ClickHouse uses a two-phase architecture: `optimizeJoin.cpp` fixes the join order first,
then Cascades optimizes only the distribution strategy. This means join order is optimized
for local cost, not distributed cost. This is an intentional tradeoff — integrating join
ordering would significantly increase the search space.

**String-based property tracking**: `isOptimizedFor`, `isEnforcedFor` use string
serialization as set keys. Classic Cascades uses structural hashing.

**Statistics dependency**: `estimateReadRowsCount` calls back into pre-Cascades code,
creating a dependency cycle.

**Task count safety valve**: A hard limit of 100,000 tasks. Classic Cascades relies on
branch-and-bound pruning as the primary bound.

---

## Known Limitations and Future Work

### Performance

1. **Replace string-based property keys** with structural hashing of `ExpressionProperties`.
2. **Decouple statistics** from pre-Cascades code.
3. **Convergence detection** instead of hard task count limit.

### Optimizer Features

4. **Join ordering in Cascades**: DPHyp for inner joins in
   [PR #98798](https://github.com/ClickHouse/ClickHouse/pull/98798); outer/semi/anti
   support needed.
5. **Predicate ordering by selectivity**: 4.2x overall TPC-H improvement (Dreseler 2020).
6. **Constant/value propagation through joins**: Extend
   [PR #98479](https://github.com/ClickHouse/ClickHouse/pull/98479) to propagate constants.
7. **Dependent group-by key elimination**: Remove redundant GROUP BY columns using
   functional dependencies from MergeTree keys.

---

## Worked Example

This section traces the optimizer through a concrete query to show how all the
pieces work together.

### Query

```sql
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_cascades_cost_config = '{"cpu_weight":1,"exchange_fixed_overhead":100,"io_weight":1,"memory_weight":1,"network_weight":1,"sequential_weight":1000}';

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
Tables: orders 150M rows, customer 12.6M rows, nation 25 rows.
After date filter: orders ~5.3M rows.

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
          JoinLogical (rows: ~5.3M)                    -- (customer ⋈ orders) ⋈ nation
            JoinLogical (rows: ~5.3M)                  -- customer ⋈ orders
              Expression
                ReadFromMergeTree (customer)            -- 12.6M rows
              Expression (WHERE o_orderdate range)
                ReadFromMergeTree (orders)              -- 5.3M rows after filter
            Expression
              ReadFromMergeTree (nation)                -- 25 rows
```

No distribution properties, no exchanges, no costs. `SortingStep` is still a logical
step — Cascades will strip it into a property.

### Memo construction

Each operator becomes a group. `SortingStep` is stripped — the sort description
`[total_revenue DESC]` becomes a required property on Group #1's input link.
`TwoPhaseAggregation` transformation creates an additional Group #12.

```
Group #0:  Expression (Project names)         -- root, required: {1 node}
Group #1:  Expression (before ORDER BY)       -- required: {1 node, sorted [revenue DESC]}
Group #2:  Aggregating (GROUP BY n_name)
Group #3:  Expression (before GROUP BY)
Group #4:  Join (join#5 ⋈ nation)             -- inputs: #5, #10
Group #5:  Join (customer ⋈ orders)           -- inputs: #6, #8
Group #6:  Expression -> customer
Group #7:  ReadFromMergeTree (customer)        -- 12.6M rows
Group #8:  Expression -> orders
Group #9:  ReadFromMergeTree (orders)          -- 5.3M rows
Group #10: Expression -> nation
Group #11: ReadFromMergeTree (nation)          -- 25 rows
Group #12: Aggregating (Partial)              -- from TwoPhaseAggregation rule
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

For **orders** (Group #9, 5.3M rows after date filter):

| Physical Expression | Distribution | Cost |
|---|---|---|
| `ParallelRead` | `{4 nodes}` | 17,960,888 |
| `ParallelRead` | `{2 nodes}` | 35,921,776 |
| `ReadFromMergeTree` (local) | `{1 node}` | 71,843,551 |
| `ShuffleExchange(o_custkey)` | `{4 nodes, by o_custkey}` | 89,904,439 |
| `ReplicatedRead` | `{4 nodes, replicated}` | 71,843,551 |

`ParallelRead` at 4 nodes is cheapest (1/4 of data per node). `ReplicatedRead` is
expensive for large tables (each node reads all 5.3M rows).

### Implementation rules: join strategies

For **customer ⋈ orders** (Group #5):

| Strategy | Distribution | Subtree Cost | Best? |
|---|---|---|---|
| **Shuffle HashJoin** (by custkey) | `{4 nodes}` | **2,930,003,018** | **Yes** |
| Shuffle HashJoin (by custkey) | `{2 nodes}` | 5,572,250,346 | |
| Local HashJoin | `{1 node}` | 10,836,555,241 | |

Shuffle wins — both tables are large. Broadcasting either side would replicate millions
of rows. Shuffling by `custkey` sends each row to exactly one node.

For **join#5 result ⋈ nation** (Group #4):

| Strategy | Distribution | Subtree Cost | Best? |
|---|---|---|---|
| **Broadcast HashJoin** (nation replicated) | `{4 nodes}` | **2,931,382,954** | **Yes** |
| Shuffle HashJoin (by nationkey) | `{4 nodes}` | 3,046,181,303 | |
| Local HashJoin | `{1 node}` | 10,847,220,977 | |

Broadcast wins — nation has 25 rows. The build side uses `ReplicatedRead` (cost 1,700)
instead of `BroadcastExchange` — each node reads the full nation table from shared
storage, avoiding network overhead entirely.

### Implementation rules: aggregation

`TwoPhaseAggregation` transformation created Group #12 (`Aggregating Partial`).

| Strategy | Distribution | Subtree Cost | Best? |
|---|---|---|---|
| **MergingAggregated -> GatherExchange -> Partial** | `{1 node}` | **2,932,971,999** | **Yes** |
| Aggregating (Shuffle, by n_name) | `{4 nodes}` | 3,883,848,251 | |

Two-phase wins — GROUP BY `n_name` has only 25 distinct values. Partial aggregation
on 4 nodes reduces 5.3M rows to ~25 per node (100 total), then `GatherExchange` sends
100 rows to one node for `MergingAggregated`. The shuffle alternative would send 5.3M
rows through `ShuffleExchange` before aggregating.

### Enforcer composition: sorting

Group #1 requires `{1 node, sorted by [total_revenue DESC]}`. The fixed-point loop
composes sorting with distribution enforcers:

1. `SortingEnforcer` creates `Sort({1 node, sorted})` from unsorted `{1 node}` expression
2. `SortingEnforcer` creates `Sort({4 nodes, sorted})` from unsorted `{4 nodes}` expression
3. `DistributionEnforcer` creates `GatherExchange(sorted)` from `Sort({4 nodes, sorted})`

Two competing plans for `{1 node, sorted}`:
- **Strategy A**: `Sort({1 node})` — sort locally after gather. Cost: 2,932,997,118
- **Strategy B**: `GatherExchange(sorted) -> Sort({4 nodes})` — sort per node, merge-gather

Strategy A wins — only 25 rows to sort after two-phase aggregation.

### Final plan

```
Expression (Project names)
  Sorting (ORDER BY total_revenue DESC)           -- on 1 node, 25 rows
    Expression
      MergingAggregated                            -- final merge on 1 node
        GatherExchange                             -- collect ~100 partial results
          Aggregating (Partial, GROUP BY n_name)   -- on 4 nodes, 5.3M -> 25 rows each
            Expression
              JoinLogical (Broadcast HashJoin)      -- nation broadcast (25 rows)
                JoinLogical (Shuffle HashJoin)      -- customer ⋈ orders by custkey
                  Expression
                    ShuffleExchange                 -- shuffle customer by custkey
                      ReadFromMergeTree (ParallelRead customer)  -- 4-way parallel
                  Expression
                    ShuffleExchange                 -- shuffle orders by custkey
                      ReadFromMergeTree (ParallelRead orders)    -- 4-way parallel
                Expression
                  ReadFromMergeTree (ReplicatedRead nation)      -- each node reads full table
```

Total subtree cost: **2,932,997,120**. Optimization: **22 ms**, **574 tasks**.

### Summary of decisions

| Decision | Choice | Why |
|---|---|---|
| customer ⋈ orders | **Shuffle** by `custkey` | Both large (12.6M, 5.3M); broadcast would replicate millions |
| result ⋈ nation | **Broadcast** with `ReplicatedRead` | 25 rows; shared storage avoids network |
| Aggregation | **Two-phase** (partial -> gather -> merge) | 25 groups; partial agg reduces 5.3M to ~25 per node |
| Sorting | **Local** (sort after gather) | 25 rows after merge agg; trivial |
| Orders read | **ParallelRead** (4 nodes) | Splits 5.3M across 4 nodes |
| Nation read | **ReplicatedRead** | 25 rows; cheaper than exchange overhead |
