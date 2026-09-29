# Cascades Optimizer — Architecture

## What Problem Does This Solve?

ClickHouse can execute a query on a single node using its standard pipeline: plan the
query, read from MergeTree, apply filters/expressions/joins/aggregations, return results.
But when data is spread across multiple nodes (shared storage like S3, or a multi-node
cluster), the optimizer must decide how to distribute the work:

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
There is no cost-based comparison of broadcast, shuffle, and local strategies.

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

`IN (subquery)` follows the `rewrite_in_to_join` setting like the rest of the planner;
Cascades does not force the join form. In the default set form the memo ingests the
plan with its `DelayedCreatingSets` placeholder as an ordinary step, and the
set-building subqueries are planned separately, distributed like any other query.

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
silently running the exchanges as no-ops locally. A search that does not finish
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

- **Distribution**: How many nodes the data is spread across, whether it is replicated,
  and which columns it is partitioned by.
  - `{1 node}` — all data on the coordinator
  - `{4 nodes}` — data split across 4 nodes (any partitioning)
  - `{4 nodes, by custkey}` — data hash-partitioned by `custkey` across 4 nodes
  - `{4 nodes, replicated}` — full copy on every node

- **Sorting**: Whether the output is sorted and by which columns.

A parent step **requires** specific properties from its child. For example, a
`ShuffleHashJoin` on `custkey` requires both inputs to be distributed `{4 nodes, by custkey}`.
If the child does not naturally produce that distribution, an **enforcer** (exchange
step) is added to bridge the gap.

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
  `ReplicatedReadImplementation`, `ReplicatedSubplanImplementation`, `TopNImplementation`,
  `DefaultImplementation`, `DistributionPassthrough`) to generate physical expressions
  with concrete properties.
- **Stage 3 — Enforce**: Apply enforcer rules (`DistributionEnforcer`, `SortingEnforcer`)
  to bridge property gaps. Uses a fixed-point loop for enforcer composition (e.g.,
  `SortingEnforcer` creates `Sort({N, sorted})`, then `DistributionEnforcer` creates
  `GatherExchange(sorted)` from it).
- **Stage 4 — Done**: Group is fully optimized for the requested properties.

The root group is optimized for `{1 node}` (the coordinator must return the final result).
Each child group is optimized for whatever properties its parent requires. The optimizer
works top-down, recursively optimizing each group. The search carries no per-subtree cost
budget: a best plan taken from a partially optimized sibling group is an upper bound, not
a lower bound, so a budget derived from it can prune plans that would become cheapest.
The search is bounded by the task budget instead, which fails closed: if optimization
does not finish within the budget, the query is rejected with a clear error (naming the
remaining task) rather than built from a partial memo.

The four stages are driven by six concrete task types on the stack: `OptimizeGroupTask`
and `ExploreGroupTask` drive a whole group; `ExploreExpressionTask` collects the matching
transformation rules and `OptimizeExpressionTask` the matching implementation rules for
one expression; `ApplyRuleTask` runs a chosen rule; and `OptimizeInputsTask` walks an
expression's inputs and costs it once they are all optimized.

Each rule carries a static **promise** (a priority). The rules applicable to an
expression are sorted by promise and pushed on the LIFO stack, so the highest-promise
rule runs first. Promise does not change which plans are considered — every applicable
rule is eventually applied — but it orders their generation, which breaks ties among
equal-cost alternatives (the first-found best is kept and a later equal-cost candidate is
pruned). Enforcer rules are scheduled by the Stage-3 fixed-point loop, so their promise
is not consulted.

**Key files**: `Task.h/cpp`, `Optimizer.h/cpp`, `CascadesParams.h/cpp`, `OptimizerContext.h`

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
`DefaultImplementation` passthroughs carry no strategy (`strategy == nullptr`). Note
that the strategy pointer does not decide whether an expression is logical or physical:
that is decided by which of the group's two lists holds it (`logical_expressions` or
`physical_expressions`), and physical expressions without a strategy are normal (a
single-node top-N, every `DefaultImplementation` product).

Strategies are grouped by operator family — `IJoinStrategy`, `IAggregationStrategy`,
`IReadStrategy` — and each concrete strategy implements `estimateOperatorCost` in
`Cost.cpp` (kept there so every cost formula lives in one file):

- Joins: `LocalJoinStrategy`, `BroadcastJoinStrategy`, `ShuffleJoinStrategy`
- Aggregation: `LocalAggregationStrategy`, `ShuffleAggregationStrategy`, `PartialAggregationStrategy`
- Reads: `ParallelReadStrategy`, `ReplicatedReadStrategy`

`estimateOperatorCost` dispatches on the family with `dynamic_cast`; a logical operator
still without a strategy is priced as its cheapest reasonable default (a non-broadcast
hash join, or local aggregation) so a partially implemented plan still gets a finite cost.

One strategy is a marker with no cost function of its own: `ReplicatedSubplanStrategy`
marks a step run identically on every node over replicated inputs; it satisfies
`{node_count=N, is_replicated=true}` without a `BroadcastExchange`. Replicated
expressions get parallelism 1.0, so the default per-step formulas already charge the
full work each node repeats.

**Key files**: `ImplementationStrategy.h`, `Cost.cpp`

### Sorting: Property or Operator?

In classical Cascades the sort order is only a physical property, enforced on demand.
This implementation is a hybrid, because the memo ingests the planner's finished step
tree one group per step:

- The required order **is** a property (`ExpressionProperties::sorting`).
  `SortingEnforcer` produces sorts on demand and composes with `DistributionEnforcer`
  in the two classical ways: gather then sort, or sort per node then sorted-merge
  gather.
- A plain `SortingStep` (`Full`, without a limit) never becomes a group: ingestion
  strips it and attaches its sort description as the required property of the parent's
  input link (`CascadesOptimizer::addGroup`), so the enforcer owns every plain sort in
  the memo.
- A `SortingStep` with a limit is a top-N: the bound changes the row count, which no
  property can express, so it is genuinely an operator with its own rules.
  `TopNImplementation` implements it on a single node, and `TwoStageTopN` splits it
  into a per-node bounded sort plus a coordinator limit over the sorted-merge gather.
  The per-node partial is marked with the planner-only `SortingStep::isPartialTopN`
  flag (not serialized, like `is_sorting_for_merge_join`): the partial emits up to
  `L` rows on each node and needs the re-bounding limit above, so it is a different
  operator from the global top-N — it may be implemented per node, and the split is
  not applied to it again.

### Cost Model

Three-component cost with `work`, `network`, and `sequential` dimensions,
combined via configurable weights:

```
total_cost = work * work_weight + network * network_weight + sequential * sequential_weight
```

- `work`: rows or bytes processed, divided by parallelism (I/O + CPU combined)
- `network`: bytes transferred between nodes
- `sequential`: single-threaded phases (gather/scatter funnels, merge cursors)

Every dimension is priced as wall-clock per node. Work divides by the parallelism of
the expression. Network follows the same rule: a shuffle moves `1/N` of the data per
node with all nodes concurrent, so it divides by the node count; a broadcast charges
its payload once (every receiver ingests all of it, in parallel); a gather or scatter
funnels every row through one endpoint, so its transfer stays undivided and each row
also pays `funnel_sequential_cost_per_row`. A hash-table build counts as work, not as
a sequential phase: `parallel_hash` shards the build across the threads of a node
(a broadcast join still builds the full table on every node, a shuffle join `1/N`).
`sequential_weight` is the per-node thread count: a serial phase holds one thread
while work spreads over all of them, so one serial row costs about `threads` work
rows (Brent's law).

In addition, every exchange adds a fixed `exchange_fixed_overhead` to `sequential`
(connection setup and metadata), which keeps a plan over a small input local. A
partial top-N is charged for scanning its whole input while its sorted gather carries
up to `limit * node_count` rows.

A table read is priced on its scan volume, not on its output: the rows the primary
key keeps (from the index analysis) times the row width, with the output estimate as
a lower bound (stat hints mark tiny stand-in tables whose physical selection says
nothing about the pretended size). A filter off the sorting key prunes no granules,
so the scan can exceed the output estimate by orders of magnitude; without this a
replicated read of such a table would look almost free.

The whole cost configuration is overridable at query time via one JSON parameter —
the three weights, the per-exchange overhead, and the calibration constants
(`expression_cost_per_row`, `hash_table_build_factor`, `unknown_leaf_cost`,
`funnel_sequential_cost_per_row`, `merge_sequential_cost_per_row`):
```sql
SET param__internal_cascades_cost_config = '{"work_weight":1,"network_weight":1,"sequential_weight":32}';
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
(`OptimizerContext`). The rules honor `distributed_aggregation_memory_efficient`
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
  never `ASOF` or `INNER ANY`, and never `ANY` under `join_any_take_last_row` (the
  kept row comes from the hash-table build side, so a swap changes the result)
- `TwoStageAggregationTransformation` — splits aggregation into partial + merge
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
- `ReplicatedSubplanImplementation` — when the parent requires a replicated result,
  re-runs a replication-safe step identically on every node over replicated inputs,
  extending replicated reads to whole subtrees without a `BroadcastExchange`.
  `ANY`/`RightAny` joins are not replication-safe: with duplicate build-side keys the
  kept row depends on the parallel build order, so nodes could produce different rows
- `TopNImplementation` — bounded sort at one node, or per node for the top-N partial
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

**Key files**: `Rules/*.cpp`, `DagNameTranslation.h/cpp`

### Statistics

Statistics are derived on-demand during rule application and cached on groups. Each
group has `estimated_row_count` (plus a proven `max_row_count`, per-column NDVs, and a
byte width) used for cost estimation. Read groups — including an initial filter directly
over a read — are prepopulated from index analysis, column statistics, or test hints;
join and aggregation statistics are derived by `StatisticsDerivation.cpp`. Join estimates
are clamped to the semantics of the join kind and strictness (an outer join keeps its
preserved side, semi/anti/any joins cannot exceed it, a paste join is position-wise).
A standalone `FilterStep` (e.g. `HAVING`) is estimated from the input column NDVs:
an equality counts as `1/NDV`, its negation as the complement, and other predicates
get default factors; `and`/`or`/`not` compose. Equality columns the plan below already
enforces (the keys of an inner join under the filter) are tracked as equivalence
classes, and an equality inside such a class removes nothing. A read also carries its
physical scan volume (see the cost model) next to the output estimate.

Row widths drive the exchange costs, so they come from measured data, not type sizes.
A read's per-column average widths come from the parts' column-data sizes (compact parts
carry only a total, so type-based estimates are scaled to match it; the total deliberately
excludes non-column files such as statistics sketches, which can dwarf a small table's
data). A test hint overrides them: per-column `column_bytes` first, else a table-level
`avg_row_bytes` distributed over the columns by type (a hinted table's physical parts are
stand-ins and are not trusted). Widths follow columns through renames only while the value
bytes are unchanged (`Nullable`/`LowCardinality` wrapping is ignored; a value-changing hop
such as `toString` drops the width to unknown, falling back to the type default — 64 bytes
for `String`). Joins and aggregations recompute their width from their actual output
header using the known column widths. The only floor is 1 byte per row, guarding against
a zero-width row (a bare `count()`) making exchanges look free.

**Key files**: `Statistics.h/cpp`, `StatisticsDerivation.cpp`

### Step digests and cross-group identity

Expressions are compared by the *content* of their step, over the canonical digests written by
`writeStepFullDigest` and `writeStepLogicalDigest` (`Processors/QueryPlan/StepIdentity.h`); this
directory's `StepIdentity.h` turns each digest into a fingerprint and a byte-exact comparison. That
header is the contract for the framing, for what is stable across calls and for the rule that digest
bytes must never be cached and compared later; it is not restated here.

There used to be a third, weaker tier - `GroupExpression::structurallyEqualTo`, comparing the step's
name and its display description - used as the within-group duplicate filter. It was a stand-in for
the missing step-content equality: two `FilterStep`s with different predicates look identical that
way, so it could drop a distinct alternative. It is deleted; the total full digest replaced it, and
no surveyed memo keeps such a tier.

One writer per digest, two consumers each: the SipHash-128 fingerprint and the byte-exact
confirmation both run over the same bytes, so a fingerprint collision never decides equality on
its own.

**Two identity levels, two jobs.** They answer different questions and must not be swapped.

| | full digest | logical digest |
|---|---|---|
| Question | are the two step objects interchangeable? | do they compute the same relation? |
| Content | wire `serialize` + `serializeSettings` bytes plus the audited non-wire extras, or a whole-object witness | the relation-defining fields only, all authored by the step |
| Totality | total: every step has one, and writing one never throws | opt-in per audited step type, fail-closed per instance |
| Frame (`GroupExpression`) | properties, inputs, `strategy`, `enforced_property`, `description_suffix`, step | properties, inputs, step |
| Job | duplicate filter *inside* one group | key of group membership |
| Step hook | `writeFullDigest` (base default: whole-object witness) | `hasLogicalDigest` / `writeLogicalDigest` |

Physical knobs (thread counts, block sizes, buffering, spill settings) are deliberately out of
the logical digest: two subtrees that differ only in `max_threads` then land in one group and
become costed alternatives, instead of one of them silently winning by entering the memo first.
Dropping one of two expressions that differ only in a knob has to stay a decision of the cost
model, which is why the full digest - and only it - may be used to drop an alternative.

`fullyEqualTo` implies `logicallyEqualTo` for every constructible step that has a logical digest,
but the implication is not structural: a logical writer may encode a field the wire encodes only
conditionally (`LimitStep::description`, `SortingStep::prefix_description`), so it rests on those
fields being empty exactly when the wire omits them - a construction invariant, not something the
digests enforce. The direction of failure would be a missed merge, never a wrong one.

A knob that is excluded from the logical digest but *gates* a relation-defining field is the one
trap this design has. `MergingAggregatedStep::memory_efficient_aggregation` is the case: the
memory-efficient merge path never applies `max_rows_to_group_by` or `bucket_top_k`, the plain path
applies both. The fix is to gate, not to include - `hasLogicalDigest` rejects the instance that
configures the truncation, so the untruncated variants still merge with each other. Same shape on
`AggregatingStep`, where the excluded two-level thresholds gate `bucket_top_k`. When adding a
field, check both directions: does the field change rows, and does an excluded field decide whether
it is read at all?

The wire bytes cannot be filtered down to the logical ones: they interleave relation-defining
and physical fields with no markers, and each step's payload is a black box. So the two writers
are separate methods over shared helpers, and neither is derived from the other.

**What a group means.** All expressions in a group are mutually substitutable for every consumer
of the group. For ordinary groups that means they compute the same relation. For a **stage-marked**
group - the partial stage a two-stage rule splits off - it means the acceptable-set semantics: a
partial aggregation or a partial top-N is not even a function of the input relation (its output
depends on how rows are spread over nodes and streams), so the group denotes the set of relations
that merge to the correct result, and the only consumers are the merge expressions built together
with it. `GroupExpression::physical_output_rows` already reflects this (a partial top-N emits up
to L rows per node while the group statistics are trimmed to L).

Stage markers are therefore relation-defining and stay in the logical digest:
`SortingStep::is_partial_top_n`, `AggregatingStep::final`, `LimitStep::is_shard_limit`, and the
`AggregatingStep` / `MergingAggregatedStep` split. This is also what keeps deduplication from
folding a partial stage back into its source group and forming a self-cycle.

**Nondeterministic operators.** Merging two identical nondeterministic subplans (a LIMIT without a
full order, `any` aggregates) forces them to produce the same rows where separate execution could
diverge. That is accepted: any merged outcome is a valid outcome of each subplan on its own, which
is standard common-subexpression-elimination semantics.

**The full digest is total; the logical digest is opt-in.** `IQueryPlanStep::writeFullDigest` has a
base default - one whole-object witness of `this` - so every step has a full digest and writing one
never throws. A step type without a content override, and a content step whose in-override guard
rejects the instance, therefore compares equal only to itself, which is pointer identity expressed
inside the single mechanism. `IQueryPlanStep::hasLogicalDigest` still defaults to `false`, and there
a step that has not opted in compares equal to nothing at all, not even to itself, since a fresh
group is the only sound outcome. Both defaults are the fail-closed mechanism of the whole feature: a
missed deduplication costs nothing, while a wrong one on a read returns wrong rows. Content overrides
are **monotone**: replacing a witness with canonical content only turns missed merges into merges, so
"digest-equal implies interchangeable" holds at every migration point.

The one place the full digest can throw is the wire encoding it embeds
(`StepDigestWriter::addStepWireEncoding`). That is why the guards live *inside* each content override,
re-checked per instance and never as a try/catch: an instance that fails them writes the witness
instead. The recurring throw classes are the `NonZeroUInt64` plan settings, a correlated
`PLACEHOLDER` node in a DAG, and `ReadFromMergeTree`'s unencodable read shapes.

**Classifying a field as relation-defining (the logical digest).** Given fixed input relations
(and, for a stage-marked step, a fixed acceptable set), does changing this field change the output
rows or the output header?

- Yes: in. Row-affecting limits and truncations count, even when they are provably exact
  (`limit_hint`, the `bucket_top_k` family, `params.top_k`, `max_rows_to_group_by` with
  `group_by_overflow_mode`, the DISTINCT and sort size limits): a truncated result is a different
  result. So do the join operator, the DAGs, the keys, and the stage markers above.
- Only cost, parallelism, memory, chunking or spilling: out.
- Only informs another optimizer pass, without changing this step's relation: out of the logical
  digest, kept in the full one, so both variants coexist in one group and the passes that read the
  flag keep working on their own expression. Examples: `prevent_input_removal`,
  `SortingStep::is_sorting_for_merge_join`, `JoinStepLogical::optimized`,
  `disjunctions_optimization_applied`, the join estimates and `table_stats_hint`, the
  query-condition-cache key.
- A sort description is in whenever it is an *order claim* (what `getSortDescription` reports) or
  an *input-order assumption* (it selects a transform that is only correct for that order):
  `ExpressionProperties` models the delivered order of a `SortingStep` only, so nothing else in the
  logical frame would carry it.
- **Layout-dependent flags** (`AggregatingStep::skip_merging`, `DistinctStep::skip_stream_merging`,
  `LimitByStep::skip_stream_merging`) mean "my input streams are disjoint on my keys". The memo does
  not model that yet, so an instance with such a flag set returns `false` from `hasLogicalDigest`.
  Once the `stream_layout` property normalization lands, the flag is stripped at ingestion into a
  `Disjoint(keys)` requirement on the input link and this opt-out goes away.
- When in doubt: the instance predicate goes false. Fail closed.

The per-field rationale lives next to each writer and its tag enum in the step `.cpp`. The two
writers of a step have **separate tag enums**; tags are unique within one writer and appear in the
same order on every call, with explicit absent slots.

**Giving a step a content full digest, or classifying a new field.**

- Classify every member of the step and of its base classes as one of: *on the wire* (written by
  `serialize` / `serializeSettings`, hence already in the digest through `addStepWireEncoding`),
  *execution-constraining non-wire* (written through `StepDigestWriter` in the same
  `writeFullDigest` override), or *derived or display-only* (excluded, with the reason).
- **Re-auditing after a master merge: diff the embedded value types, not only the step headers.**
  A field added to a struct a step holds *by value* enters that step's state without the step
  header changing at all, so a sweep of the audited `.h` files alone reports "no drift" and misses
  it. Diff the defining header of every value-typed member too - `JoinSettings` and
  `SortingSettings` for `JoinStepLogical`, `Aggregator::Params` for `AggregatingStep` and
  `MergingAggregatedStep`, `SortingStep::Settings`, `SelectQueryInfo` and the snapshot types for
  `ReadFromMergeTree` - and classify each addition like any other new member. One field can land
  in several steps at once and need a different answer in each: a `Params` member that constrains
  `AggregatingStep` may be inert in `MergingAggregatedStep`, whose merge path never reads it.
- A step with no wire `serialize` at all is not thereby excluded: its content digest is simply
  extras-only, over the shared preamble. The four exchange steps are that case.
- `input_headers` is excluded for every step: `GroupExpression::fullyEqualTo` compares the
  ordered child groups separately, and a step holding an `ActionsDAG` carries its inputs' names
  and types on the wire inside the serialized DAG anyway.
- Wire-absence is never on its own a reason to exclude a field — check what the step's
  pipeline-building and analysis paths, and the optimizer passes that inspect the step, actually
  read. A field deliberately kept off the wire because a remote node re-derives it, or because
  losing an optimization there is the safe direction, is usually an extra: neither argument
  transfers to identity, where the two steps both run locally.
- The digest always serializes with `for_cache_key = false`, on both the `Serialization` context
  and the sets registry (set in `Processors/QueryPlan/StepIdentity.cpp`), so a field the wire format
  gates on `!for_cache_key` — `AggregatingStep::final`, the stats cache key, runtime-filter id
  values — counts as *on the wire* for the audit and needs no tag.
- The override's guard must reject - into the whole-object witness - every instance whose
  `serialize` **or** `serializeSettings` would throw, and must establish that without a try/catch.
  The recurring `serializeSettings` throw class is the `NonZeroUInt64` plan settings: assigning a
  zero throws `BAD_ARGUMENTS`, even for a setting whose value is later overwritten and never
  reaches the wire.
- A step holding an `ActionsDAG` guards with `!hasCorrelatedExpressions()`, because
  `ActionsDAG::serialize` throws on a `PLACEHOLDER` node; a step holding several DAGs must check
  every DAG the digest writes, not just the one that predicate looks at. Known residual gap:
  `hasCorrelatedColumns` is non-recursive, so a `PLACEHOLDER` inside a `FunctionCapture` sub-DAG
  escapes it, and `ActionsDAG::serialize` has further throw paths (duplicate nodes, unexpected
  constant columns). Both are pre-existing and shared with the distributed wire path, to be
  tracked in an upstream issue; closing them is a precondition for enabling memo-wide
  deduplication.
- When in doubt, the guard rejects the instance and the field goes into the extras.
- `hasLogicalDigest` inherits only the guards that still apply. The logical digest calls neither
  `serialize` nor `serializeSettings`, so the whole `NonZeroUInt64` class disappears from it - a
  `SortingStep` built by `optimizeGroupByTopK` has a logical digest and no full one. The
  correlated-`PLACEHOLDER` guard stays wherever the logical writer serializes a DAG.

**Provenance witnesses.** State with no serialization at all (`KeyCondition`, `PartitionPruner`,
part and settings snapshots, the query tree) is encoded as the address of an object the step owns
through a `shared_ptr` (`StepDigestWriter::addWitness`). An equal address means literally the same
object, hence equal content; a different address makes the two steps unequal even when their contents
match, which costs a deduplication but never produces a wrong one. This is sound only because
equality is decided by re-digesting two live steps, so no address can be recycled behind the
comparison. The expected consequence is a narrow merge scope: in the *full* digest of a
`ReadFromMergeTree`, only reads that have not been analyzed yet and that share the part, mutation and
metadata snapshots can merge.

The same mechanism at whole-object scale is the base `writeFullDigest` default
(`addWholeObjectWitness`), which uses a reserved tag out of the range of any step's own tags, so a
witness digest can never collide with a content digest of the same step type.

**The read's logical digest: content where the full digest witnesses.** Two table expressions over
one table build their own `SelectQueryInfo`, their own storage snapshot and their own part-list
object, so witnesses can never merge them - which is why a self-join never deduplicated. The logical
digest therefore describes the relation-defining state as content: the storage identity, the metadata
version, and a digest of the part list - per part its name, data and metadata version, query-wide
numbering and mark ranges. The soundness basis is that a part name identifies the part's content on
one server within one query (every merge, mutation and lightweight update writes a part under a new
name, and block numbers are never reused); it costs O(parts) per encoding, which runs on intern and
on confirmation, not per row. Witnesses remain only for state that decides rows and has no canonical
encoding: the context (from which the reader settings and the sampling decision derive), the storage
settings, the storage limits list, the metadata object, the storage object, and the two rewrite hooks
(`lazy_materializing_rows`, `virtual_row_conversion`, both null on an ordinary read). They set the
merge scope: the metadata, storage and settings objects are shared table-wide, but the context and
the limits list are per **query block**, so the table expressions of one block merge (the self-join
case) while two identical subqueries do not - each subquery is planned with a context copy of its
own. Encoding the truncating size limits and the read-relevant settings as content is what would
widen that.

Two consequences worth knowing. First, pruning is relation-defining for a read even though it is
"only" pruning: skipping a granule drops rows that fail a filter which the read's own output is
expected to still contain, so both pushed-down filter DAGs, the runtime-filter descriptors, the TopK
stamp, the vector-search parameters and the partition-pruning flag are all in. What is excluded is
the *memoized* result of pruning (`indexes`, `analyzed_result_ptr`): it is a function of state that is
in the digest, and the cases where granule skipping is not transparent are gated or encoded instead.
Second, that exclusion is what makes a read's logical digest stable over its lifetime, while its full
digest is not: the memoized analysis members are the only ones that populate lazily. The
insertion-time-fingerprint rule of the memo index still holds for every step type.

The read's instance gates (`hasLogicalDigest`) are the full digest's read-shape guards plus: a pinned
block-number boundary, a mutations snapshot carrying patch parts or pending data/alter/metadata
mutations (state that the part list cannot express - a patch part is a part of its own that changes
the rows of an unchanged part list), a part list carrying analysis residue, and a read whose SAMPLE
could still be hiding in the select AST.

### Memo-wide group deduplication

`Memo::internExpression` is the group-creation entry point for plan ingestion
(`CascadesOptimizer::addGroup`) and for every rule that splits off a stage
(`IOptimizationRule::addTwoStageSplit`). It looks the incoming expression up in a memo-wide index
of interned logical expressions: on a hit the expression joins the group that already computes that
relation and that group's id is returned, on a miss a new group is created and indexed. Guarded by
`cascades_memo_deduplication`, experimental and off by default; with the setting off the entry point
creates a group unconditionally, exactly as before.

- **The key** is `GroupExpression::logicalFingerprint`, 128 bits over the logical frame (own
  properties, the ordered inputs with their required properties, the step's logical digest). It is
  computed once, at insertion, and stored in the index entry: an entry is never looked up or removed
  by a recomputed fingerprint, because lazily populated analysis state changes a step's digest over
  its lifetime. A candidate from the fingerprint bucket is confirmed by `logicallyEqualTo`, which
  compares the frame field by field and re-digests the two live steps, so a fingerprint collision
  costs a comparison and can never merge two groups.
- **Inputs must be final** before an expression is interned - ingestion recurses before inserting,
  and a rule inherits the source expression's inputs. An input group id that changed afterwards
  would strand the entry under a fingerprint nothing can find again.
- **Only pure logical expressions** participate (`strategy == nullptr`, no `enforced_property`,
  asserted): an enforcer computes its input's relation, so it would fold into its own child group.
  Stage markers in the logical digest block the other cycle class, a partial stage folding into its
  source group. After every hit a debug assertion walks the inserted expression's input links and
  checks the target group is not reachable from them.
- **Statistics** stay the existing group's on a hit; in debug builds the two independent estimates
  are asserted to agree within a loose factor, since a real disagreement means the identity
  classified two different relations as equal. If the group has none and the incoming expression
  does, the group adopts it - the other direction would be silent information loss.
- **Within the group** the incoming expression is still filtered by the full identity, so a knob
  variant survives as a costed alternative and only a fully-equal expression is dropped. Exact cost
  ties therefore become common, and `Group::updateBestImplementation` resolves them deterministically
  in favour of the earlier-inserted expression.
- **Duplicate groups are detected, not merged.** A rule inserting into group A an expression that
  matches an interned expression of group B proves A and B equal; `Memo::addLogicalExpressionToGroup`
  counts that event and logs both ids. Merging is a later stage. Such rule-inserted alternatives are
  themselves never indexed - they are detection-only, and only a group-creating intern registers an
  index entry.
- **Counters**, in `MemoCounters` on `OptimizerContext` and logged at the end of the pass:
  `groups_created`, `groups_reused`, `duplicate_group_detections`, plus the orphan count
  (`countGroupsUnreachableFrom` over the root group), which must stay zero - interning the partial
  group of a two-stage split is what closed the one orphan this memo had.

**Remaining preconditions for enabling deduplication by default.**

- The upstream `hasCorrelatedColumns` / `ActionsDAG::serialize` gap above.
- The performance gate: per-run `StepDigestCounters` on `OptimizerContext` plus the
  `CascadesStepDigests` / `CascadesStepDigestBytes` / `CascadesStepDigestConfirmations`
  ProfileEvents ship the digest counters, but the wall-time measurement and the threshold it has
  to clear do not exist yet. That gate has to cover the default path too, not just
  `cascades_memo_deduplication = 1`: within-group deduplication digests every insertion regardless
  of the setting, and a fingerprint hit (the enforcer-duplicate case) materializes both byte digests
  to confirm it.
- Group merging (a rule inserting into group A an expression that proves A equal to B) is still only
  detected and counted, so a duplicate group that deduplication cannot prevent up front stays.

**Key files**: `StepIdentity.h/cpp`, `Processors/QueryPlan/StepIdentity.h/cpp`, `Memo.h/cpp`, and the
`writeFullDigest` override and `hasLogicalDigest` / `writeLogicalDigest` pair of each audited step

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

**Structural property hashing**: `isOptimizedFor`, `isEnforcedFor`, and expression
deduplication use `ExpressionPropertiesHash` for O(1) lookup. `GroupExpression::fullFingerprint`
(the bucket key of the within-group duplicate filter) combines properties, input group IDs with
their required properties, strategy, enforced property, description suffix and the step's full
digest fingerprint via `boost::hash_combine` — the same components, in the same order, that
`fullyEqualTo` compares.

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
3. **Pruned expressions keep no cost**: same-group best-cost pruning drops an
   expression before its cost is recorded, but plan extraction considers only costed
   expressions as acyclic fallbacks for self-referential enforcers. A cheaper
   over-providing enforcer can therefore prune the only fallback a later composition
   needs. Recording the cost before the early return closes the gap.

### Cost Model

4. **Range selectivity over aggregate results**: a `HAVING` range predicate
   (`sum(x) > c`) gets the same default factor as a base-column range, which can
   overestimate it by orders of magnitude; no statistics can exist for an aggregate
   output.
5. **`arrayJoin` fan-out**: an `ExpressionStep` with `arrayJoin` grows the row count,
   which is not estimated yet.
6. **Build-side choice of a shuffle join is a near-tie**: with the build priced as
   parallel work, the two orientations of a shuffle join differ by well under a
   percent, so the chosen build side is effectively arbitrary. Measured plans show the
   orientations perform alike, but the choice can flip between runs and builds, which
   makes plan-shape tests fragile. A size-aware hash cost (a table beyond the cache
   costs more per row) would restore a principled asymmetry.
7. **Scan-volume refinements**: the scan estimate charges every touched column at
   granule volume. Measured scans show three bounded deviations: a size-only string
   predicate (`s <> ''`) reads only the string sizes; a scattered point-key selection
   decompresses whole compression blocks per granule; and a lazily materialized
   payload column is read for fewer rows than the granule selection.

### Optimizer Features

8. **`ReplicatedRead` validation and gating**: the rule assumes every worker sees the
   same complete table (shared storage) and has no hard size gate; an oversized
   replicated read loses only on cost.
9. **Runtime bloom filter placement in Cascades**: the pre-Cascades pass may add
   `BuildRuntimeFilterStep` and Cascades passes it through, but Cascades does not
   create or cost runtime-filter alternatives. The single biggest gap vs StarRocks.
10. **Join ordering in Cascades**: the pre-Cascades join orderer already offers `dphyp`
   (inner joins) alongside `dpsub`, `dpsize`, and `greedy`; in an algorithm chain such as
   `dphyp,greedy`, unsupported cases fall through to the next algorithm. Cascades still
   receives a single fixed order; feeding the top k orders into the memo (the hybrid TOP-K
   of Gretscher & Dittrich, PVLDB 2025) is future work.
11. **Window function distribution**: `WindowStep` currently goes through
   `DefaultImplementation` at `{1 node}`. Needs a `WindowImplementation` rule
   that sets distribution by PARTITION BY key.
12. **CTE / common subplan sharing**: Detect `CommonSubplanReferenceStep` and
   map to existing groups instead of cloning.
13. **Dependent group-by key elimination**: Remove redundant GROUP BY columns using
   functional dependencies from MergeTree keys.
14. **Width through same-type value-changing functions**: a deterministic `String` ->
   `String` function (`substring`, `hex`) keeps the source column's average width even
   though the bytes change; width preservation needs a value-pass-through rule separate
   from the NDV rule.
15. **Reads from remote shards**: a read from a `Distributed` table (or the `remote`/
   `cluster` table functions) with genuinely remote shards is rejected up front
   (`checkStepSupportedByCascades`); localhost shards are inlined and planned normally.
   Supporting it as a coordinator-pinned single-task source is future work.

---

## Worked Example

This section traces the optimizer through a concrete query to show how all the
pieces work together. The cost numbers come from an older calibration of the model,
so treat them as illustrations of the comparisons, not as current values; the chosen
shapes are unchanged.

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

`CascadesOptimizer::addGroup` assigns ids in post-order — it recurses into a step's
children before inserting the step's own expression, so a child always gets a lower id
than its parent and the root group gets the highest id. The ids below are numbered
top-down purely for readability; the actual ids `addGroup` assigns run bottom-up.

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
