# Cascades Optimizer — Architecture

## Overview

The Cascades optimizer handles distributed query planning: given a logical query plan
(with a fixed join order), it decides distribution strategies (broadcast, shuffle, local),
exchange placement, sort enforcement, and parallel read strategies. It uses a
cost-based search with the classic memo/group/expression structure from the Cascades
framework (Graefe 1995).

Join ordering is done by a separate pre-Cascades pass (`optimizeJoin.cpp`) using
dpsize or greedy algorithms. The Cascades optimizer receives the fixed join tree and
optimizes only the physical execution strategy.

## Architecture

### Memo and Groups

Standard Cascades memo. Each group contains logical expressions (representing equivalent
relational subexpressions) and physical expressions (with specific implementations and
properties). Statistics are shared per group.

**Key files**: `Memo.h/cpp`, `Group.h/cpp`, `GroupExpression.h/cpp`

### Physical Properties

Two property dimensions:

- **Distribution**: `node_count`, `is_replicated`, `columns` (with equivalence sets
  for join-key aliasing). Describes how data is partitioned across nodes.
- **Sorting**: `SortDescription` + `sort_limit`. Stripped from the memo at construction
  time (`SortingStep::Full` becomes a property on the input link, not a group member).

Classic Cascades treats sorting similarly (as a physical property, not a logical step).
The distribution column equivalence sets are an extension beyond basic Cascades, needed
to avoid unnecessary reshuffles when join keys create column aliases.

**Key files**: `Properties.h/cpp`

### Cost Model

Multi-component cost with `cpu`, `memory`, `network`, `io`, and `sequential` dimensions,
combined via configurable weights. This is more expressive than classic Cascades' single
scalar cost — necessary for distributed planning where network and sequential bottlenecks
matter independently of CPU cost.

**Key files**: `Cost.h/cpp`

### Task-Based Optimization

The optimizer uses a LIFO task stack (standard Cascades pattern):

- **Stage 1 — Explore**: `ExploreGroupTask` / `ExploreExpressionTask` fire transformation
  rules (`JoinCommutativity`, `TwoPhaseAggregation`) to generate logically equivalent
  expressions.
- **Stage 2 — Implement**: `OptimizeExpressionTask` / `ApplyRuleTask` fire implementation
  rules (`HashJoinImplementation`, `AggregationImplementation`, `ParallelReadImplementation`,
  `DefaultImplementation`, `DistributionPassthrough`) to generate physical expressions with
  concrete properties.
- **Stage 3 — Enforce**: `OptimizeGroupTask` applies enforcer rules (`DistributionEnforcer`,
  `SortingEnforcer`) to bridge property gaps between what implementations produce and what
  parents require.
- **Stage 4 — Done**: Group is fully optimized for the requested properties.

**Key files**: `Task.h/cpp`, `OptimizerContext.h/cpp`

### Enforcer Scheduling

The Stage 3 gate uses `!isEnforcedFor(required_properties)`, ensuring enforcers run exactly
once per (group, properties) pair regardless of whether a satisfying implementation already
exists. This lets enforcer-created plans compete on cost with existing implementations.

A fixed-point loop within Stage 3 handles **enforcer composition**: the loop iterates over
newly-added physical expressions until no new enforcers are produced. This enables
compositions like `SortingEnforcer` creating `Sort({N nodes, sorted})` followed by
`DistributionEnforcer` creating `GatherExchange(sorted)` — producing the distributed sort
pattern `GatherExchange(sorted) → Sort({N nodes}) → subtree({N nodes})` which competes
on cost with the local alternative.

The dedup key includes sorting state so that `DistributionEnforcer` fires separately for
sorted vs unsorted source expressions.

**Pruning** at the top of `OptimizeGroupTask` uses `isExplored() && isOptimizedFor()` to
prevent re-entry loops from self-referential enforcers.

### Branch-and-Bound

Cost limits are propagated through tasks. `OptimizeInputsTask` computes child cost limits
by subtracting already-optimized sibling costs from the parent's budget. Standard technique,
aligned with classic Cascades.

### `best_implementations` Index

Best implementations per group are stored in an `unordered_map<UInt64, vector<GroupExpressionPtr>>`
keyed by distribution shape `(node_count, is_replicated)`. This gives O(1) bucket lookup
followed by a small linear scan within the bucket (typically 2-5 entries with different
column distributions or sorting variants).

The Pareto frontier is maintained: when a new implementation is added, dominated entries
(same or broader properties at higher cost) are removed.

### Rules

**Transformation rules** (generate logically equivalent expressions):
- `JoinCommutativity` — swaps join sides
- `TwoPhaseAggregation` — splits aggregation into partial + merge for distributed execution

**Implementation rules** (generate physical expressions with properties):
- `HashJoinImplementation` — local, broadcast, and shuffle join strategies
- `AggregationImplementation` — single-node and distributed aggregation
- `ParallelReadImplementation` — parallel read across N nodes
- `ReplicatedReadImplementation` — full table read on each node (shared storage)
- `DefaultImplementation` — default single-node implementation for any step
- `DistributionPassthrough` — passthrough distribution for stateless per-row steps
  (`ExpressionStep`, `FilterStep`, `BuildRuntimeFilterStep`)

**Enforcer rules** (bridge property gaps):
- `DistributionEnforcer` — adds `GatherExchange`, `BroadcastExchange`, `ShuffleExchange`,
  `ScatterExchange` steps. Produces both regular and sorted-merge gather variants.
- `SortingEnforcer` — adds `SortingStep` with the required sort description.

**Key files**: `Rules/*.cpp`

### Statistics

Statistics are derived on-demand during rule application and cached on groups. Each group
has an `ExpressionStatistics` with `estimated_row_count`. Statistics derivation walks
the expression tree bottom-up using `StatisticsDerivation.cpp`.

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
| `best_implementations` lookup | Aligned | Indexed by distribution shape for O(1) bucket lookup |

### Differences from classic Cascades

**Join ordering outside Cascades**: The most significant architectural difference. In
classic Cascades (Columbia, GPORCA, CockroachDB, StarRocks), join ordering IS the
Cascades optimization — join associativity, commutativity, and reordering are transformation
rules that generate alternative join trees in the memo. The cost model picks the best
join order AND distribution simultaneously.

ClickHouse uses a two-phase architecture: dpsize/greedy fixes the join order, then
Cascades optimizes distribution. This means:
- Join order is optimized for local cost, not distributed cost
- Distribution-aware join reordering (e.g., prefer joins that avoid shuffles) is impossible
- The two cost models (dpsize and Cascades) use different statistics

This is an intentional design tradeoff. Integrating join ordering into Cascades would
require implementing join associativity/commutativity as transformation rules and ensuring
the search space doesn't explode.

**String-based property tracking**: `isOptimizedFor`, `isEnforcedFor`, `isFullyDoneFor`
use `required_properties.dump()` (string serialization) as set keys. This involves string
allocation, hashing, and comparison on every `OptimizeGroupTask` entry (thousands of times).
Classic Cascades uses structural comparison or hashing of the property struct directly.

**Statistics dependency on pre-Cascades code**: `estimateReadRowsCount` in `Statistics.cpp`
calls back into `optimizeJoin.cpp` code, creating a dependency cycle between the two
frameworks. Classic Cascades derives all statistics within its own framework.

**Task count safety valve**: A hard limit of 100,000 tasks serves as the primary safety
valve. It doesn't distinguish between "converged optimally" and "ran out of budget."
Classic Cascades relies on branch-and-bound cost pruning to bound exploration, with task
limits only as a fallback. Better convergence detection (e.g., tracking whether new best
plans are still being discovered) would improve reliability.

---

## Known Limitations and Future Work

### Performance

1. **Replace string-based property keys** with structural hashing of `ExpressionProperties`.
   Eliminates string allocation on every task entry.

2. **Decouple statistics** from pre-Cascades code. Remove `estimateReadRowsCount` callback
   into `optimizeJoin.cpp`; derive all statistics within the Cascades framework.

3. **Convergence detection** instead of hard task count limit. Track whether new best plans
   are being discovered; stop early when the search has converged.

### Optimizer Features

4. **Join ordering in Cascades**: Move join order exploration into the Cascades framework
   using transformation rules (associativity, commutativity). This would enable
   distribution-aware join reordering. DPHyp is available for inner joins in
   [PR #98798](https://github.com/ClickHouse/ClickHouse/pull/98798); outer/semi/anti
   support is the next step.

5. **Predicate ordering by selectivity**: Reorder conjunctive predicates in filter steps
   by estimated selectivity. Dreseler (VLDB 2020) measured 4.2x overall TPC-H improvement
   from predicate placement. The selectivity estimation infrastructure exists
   (`ConditionSelectivityEstimator`); needs to be applied to filter ordering and PREWHERE
   selection.

6. **Constant/value propagation through joins**: Extend the transitive predicate
   infrastructure ([PR #98479](https://github.com/ClickHouse/ClickHouse/pull/98479)) to
   propagate constant and range constraints through join equivalence classes
   (e.g., `A.key=50 AND A.key=B.key` → `B.key=50`).

7. **Dependent group-by key elimination**: Detect functional dependencies from MergeTree
   ordering keys and remove redundant columns from aggregation hash tables. Affects 7
   TPC-H queries (Q3, Q4, Q10, Q13, Q18, Q20, Q21).

---

## Worked Example

This section traces the optimizer through a concrete query to show how memo construction,
implementation rules, enforcer composition, and cost-based selection work together.

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

### Input from join order optimizer

Before the Cascades optimizer runs, `optimizeJoin.cpp` fixes the join order:

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

At this point: no distribution properties, no exchanges, no cost estimates.
`SortingStep` is present as a logical step — the Cascades optimizer will strip it.

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
instead of `BroadcastExchange` — each node reads the full nation table from shared storage.

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
