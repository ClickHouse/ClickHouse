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
