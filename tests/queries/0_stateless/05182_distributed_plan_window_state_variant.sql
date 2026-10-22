-- A window aggregate of the CrossTab family produces its state in the Window representation, and no
-- header carries the representation. Below a sorted gather the local in-memory exchange handed such
-- states to a consumer whose header named the Aggregation representation, and inside a deserialized
-- fragment a cast of such a state was built for the Aggregation representation; reading the one layout
-- as the other segfaulted in CrossTabCountsState::merge. Results must match the non-distributed plan.

DROP TABLE IF EXISTS t_window_state_variant_gather;

CREATE TABLE t_window_state_variant_gather (p UInt32, b UInt32, c UInt32, v UInt32)
ENGINE = MergeTree ORDER BY (p, v) SETTINGS index_granularity = 256;

-- Several parts and several partitions, so the window runs per bucket below the sorted gather. b and c
-- both vary inside a partition, otherwise every finalized value would collapse to 0 and the comparison
-- against the non-distributed plan would hold no matter what the states contained.
INSERT INTO t_window_state_variant_gather SELECT number % 4, number % 3, intDiv(number, 3) % 5, number FROM numbers(40);
INSERT INTO t_window_state_variant_gather SELECT number % 4, number % 3, intDiv(number, 3) % 5, number + 1000 FROM numbers(40);
INSERT INTO t_window_state_variant_gather SELECT number % 4, number % 3, intDiv(number, 3) % 5, number + 2000 FROM numbers(40);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
    distributed_plan_default_shuffle_join_bucket_count = 8, distributed_plan_default_reader_bucket_count = 8,
    optimize_read_in_order = 0, optimize_sorting_by_input_stream_properties = 1,
    distributed_plan_optimize_exchanges = 1, max_threads = 8, max_rows_to_group_by = 0;

-- The states must cross the exchange below a SORTED gather: that is the shape which puts a
-- MergingSortedTransform, built from the consumer's header, over what the exchange delivers. Assert that
-- both plan nodes are present and that the gather is above the window; a bare position comparison would
-- pass with the gather missing, because minIf returns 0 when nothing matches. The checking query runs
-- non-distributed, because an aggregating query over EXPLAIN would itself be distributed.
SELECT 'sorted gather above the window step: ',
    countIf(explain LIKE '%GatherExchange (sorted by%') > 0
    AND countIf(explain LIKE '%Window (Window step%') > 0
    AND minIf(n, explain LIKE '%GatherExchange (sorted by%') < minIf(n, explain LIKE '%Window (Window step%')
FROM
(
    SELECT explain, rowNumberInAllBlocks() AS n
    FROM
    (
        EXPLAIN SELECT p, v, contingencyState(b, c) OVER (PARTITION BY p ORDER BY v)
        FROM t_window_state_variant_gather ORDER BY p, v
        SETTINGS make_distributed_plan = 1
    )
)
SETTINGS make_distributed_plan = 0;

SELECT '-- contingency, distributed: finalized values';
SELECT p, v, round(finalizeAggregation(s), 6) AS f
FROM (SELECT p, v, contingencyState(b, c) OVER (PARTITION BY p ORDER BY v) AS s FROM t_window_state_variant_gather)
ORDER BY p, v LIMIT 5;

-- The whole column, distributed and non-distributed; both lines must show the same value.
SELECT '-- contingency: distributed then local';
SELECT sum(cityHash64(p, v, round(finalizeAggregation(s), 6)))
FROM (SELECT p, v, contingencyState(b, c) OVER (PARTITION BY p ORDER BY v) AS s FROM t_window_state_variant_gather);
SELECT sum(cityHash64(p, v, round(finalizeAggregation(s), 6)))
FROM (SELECT p, v, contingencyState(b, c) OVER (PARTITION BY p ORDER BY v) AS s FROM t_window_state_variant_gather)
SETTINGS make_distributed_plan = 0;

-- The -Map combinator keeps one nested state per key, and each of them must be converted.
SELECT '-- contingencyMap: distributed then local';
SELECT sum(cityHash64(p, v, mapApply((k, x) -> (k, round(x, 6)), finalizeAggregation(s))))
FROM (SELECT p, v, contingencyMapState(b, c, p) OVER (PARTITION BY p ORDER BY v) AS s FROM t_window_state_variant_gather);
SELECT sum(cityHash64(p, v, mapApply((k, x) -> (k, round(x, 6)), finalizeAggregation(s))))
FROM (SELECT p, v, contingencyMapState(b, c, p) OVER (PARTITION BY p ORDER BY v) AS s FROM t_window_state_variant_gather)
SETTINGS make_distributed_plan = 0;

-- theilsU is the negative control: its window state keeps the ordinary state's leading members, so
-- reading it as the ordinary state never crashed and it must keep working either way.
SELECT '-- theilsU control: distributed then local';
SELECT sum(cityHash64(p, v, round(finalizeAggregation(s), 6)))
FROM (SELECT p, v, theilsUState(b, c) OVER (PARTITION BY p ORDER BY v) AS s FROM t_window_state_variant_gather);
SELECT sum(cityHash64(p, v, round(finalizeAggregation(s), 6)))
FROM (SELECT p, v, theilsUState(b, c) OVER (PARTITION BY p ORDER BY v) AS s FROM t_window_state_variant_gather)
SETTINGS make_distributed_plan = 0;

-- A window state merged with an ordinary one through a Variant: the cast to the Variant runs in the
-- fragment that holds the window, and has to convert the state rather than relabel it.
SELECT '-- Variant of a window and an ordinary state: distributed then local';
SELECT round(contingencyMerge(s.`AggregateFunction(contingency, UInt32, UInt32)`), 6)
FROM
(
    SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1
    UNION ALL
    SELECT contingencyState(b, c) AS s FROM t_window_state_variant_gather
);
SELECT round(contingencyMerge(s.`AggregateFunction(contingency, UInt32, UInt32)`), 6)
FROM
(
    SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1
    UNION ALL
    SELECT contingencyState(b, c) AS s FROM t_window_state_variant_gather
)
SETTINGS make_distributed_plan = 0;

SELECT 'Variant shape distributes'
FROM
(
    EXPLAIN PIPELINE SELECT round(contingencyMerge(s.`AggregateFunction(contingency, UInt32, UInt32)`), 6)
    FROM
    (
        SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1
        UNION ALL
        SELECT contingencyState(b, c) AS s FROM t_window_state_variant_gather
    )
    SETTINGS make_distributed_plan = 1
)
WHERE explain LIKE '%ReadFromDistributedPlanSource%' LIMIT 1
SETTINGS make_distributed_plan = 0;

SELECT '-- Dynamic: distributed then local';
SELECT round(contingencyMerge(dynamicElement(CAST(s AS Dynamic), 'AggregateFunction(contingency, UInt32, UInt32)')), 6)
FROM (SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1);
SELECT round(contingencyMerge(dynamicElement(CAST(s AS Dynamic), 'AggregateFunction(contingency, UInt32, UInt32)')), 6)
FROM (SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1)
SETTINGS make_distributed_plan = 0;

SELECT '-- CAST to the state type: distributed then local';
SELECT round(contingencyMerge(CAST(s AS AggregateFunction(contingency, UInt32, UInt32))), 6)
FROM (SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1);
SELECT round(contingencyMerge(CAST(s AS AggregateFunction(contingency, UInt32, UInt32))), 6)
FROM (SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1)
SETTINGS make_distributed_plan = 0;

SELECT '-- contingencyMerge of the window state: distributed then local';
SELECT round(contingencyMerge(s), 6)
FROM (SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1);
SELECT round(contingencyMerge(s), 6)
FROM (SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1)
SETTINGS make_distributed_plan = 0;

-- The -Tuple combinator nests one window state per element in its own state.
SELECT '-- contingencyTupleMerge of the window state: distributed then local';
SELECT round(contingencyTupleMerge(s).1, 6)
FROM (SELECT contingencyTupleState(tuple(b), tuple(c)) OVER () AS s FROM t_window_state_variant_gather LIMIT 1);
SELECT round(contingencyTupleMerge(s).1, 6)
FROM (SELECT contingencyTupleState(tuple(b), tuple(c)) OVER () AS s FROM t_window_state_variant_gather LIMIT 1)
SETTINGS make_distributed_plan = 0;

SELECT '-- lambda capture of the window state: distributed then local';
SELECT round(contingencyMerge(arrayElement(arrayMap(x -> s, [1]), 1)), 6)
FROM (SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1);
SELECT round(contingencyMerge(arrayElement(arrayMap(x -> s, [1]), 1)), 6)
FROM (SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1)
SETTINGS make_distributed_plan = 0;

SELECT '-- lambda over the window state: distributed then local';
SELECT arrayMap(x -> round(finalizeAggregation(x), 6), [s])
FROM (SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1);
SELECT arrayMap(x -> round(finalizeAggregation(x), 6), [s])
FROM (SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1)
SETTINGS make_distributed_plan = 0;

SELECT '-- lambda with the window state as argument and capture: distributed then local';
SELECT round(contingencyMerge(arrayFirst(x -> finalizeAggregation(s) >= 0, [s])), 6)
FROM (SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1);
SELECT round(contingencyMerge(arrayFirst(x -> finalizeAggregation(s) >= 0, [s])), 6)
FROM (SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1)
SETTINGS make_distributed_plan = 0;

SELECT '-- lambda capturing a lambda over the window state: distributed then local';
SELECT round(contingencyMerge(arrayElement(arrayElement(arrayMap(x -> arrayMap(y -> s, [x]), [1]), 1), 1)), 6)
FROM (SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1);
SELECT round(contingencyMerge(arrayElement(arrayElement(arrayMap(x -> arrayMap(y -> s, [x]), [1]), 1), 1)), 6)
FROM (SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1)
SETTINGS make_distributed_plan = 0;

-- A spilled block is read back under a header that names the window state inside an Array or a Tuple. Plan
-- optimizations would build the array above the sort, and the spill threshold is divided by the streams.
SELECT '-- array of window states through an external sort: spilled then not spilled';
SELECT sum(cityHash64(v, arrayMap(x -> round(finalizeAggregation(x), 6), a)))
FROM (SELECT v, [contingencyState(b, c) OVER (PARTITION BY p ORDER BY v)] AS a FROM t_window_state_variant_gather ORDER BY v DESC)
SETTINGS make_distributed_plan = 0, query_plan_enable_optimizations = 0, max_bytes_before_external_sort = 1000,
    max_bytes_ratio_before_external_sort = 0, max_block_size = 8;
SELECT sum(cityHash64(v, arrayMap(x -> round(finalizeAggregation(x), 6), a)))
FROM (SELECT v, [contingencyState(b, c) OVER (PARTITION BY p ORDER BY v)] AS a FROM t_window_state_variant_gather ORDER BY v DESC)
SETTINGS make_distributed_plan = 0, query_plan_enable_optimizations = 0, max_bytes_before_external_sort = 0,
    max_bytes_ratio_before_external_sort = 0, max_block_size = 8;

SELECT '-- tuple of window states through an external sort: spilled then not spilled';
SELECT sum(cityHash64(v, round(finalizeAggregation(a.1), 6)))
FROM (SELECT v, tuple(contingencyState(b, c) OVER (PARTITION BY p ORDER BY v), v) AS a FROM t_window_state_variant_gather ORDER BY v DESC)
SETTINGS make_distributed_plan = 0, query_plan_enable_optimizations = 0, max_bytes_before_external_sort = 1000,
    max_bytes_ratio_before_external_sort = 0, max_block_size = 8;
SELECT sum(cityHash64(v, round(finalizeAggregation(a.1), 6)))
FROM (SELECT v, tuple(contingencyState(b, c) OVER (PARTITION BY p ORDER BY v), v) AS a FROM t_window_state_variant_gather ORDER BY v DESC)
SETTINGS make_distributed_plan = 0, query_plan_enable_optimizations = 0, max_bytes_before_external_sort = 0,
    max_bytes_ratio_before_external_sort = 0, max_block_size = 8;

SELECT '-- contingencyTuple window state through an external sort: spilled then not spilled';
SELECT sum(cityHash64(v, round(finalizeAggregation(s).1, 6)))
FROM (SELECT v, contingencyTupleState(tuple(b), tuple(c)) OVER (PARTITION BY p ORDER BY v) AS s FROM t_window_state_variant_gather ORDER BY v DESC)
SETTINGS make_distributed_plan = 0, query_plan_enable_optimizations = 0, max_bytes_before_external_sort = 1000,
    max_bytes_ratio_before_external_sort = 0, max_block_size = 8;
SELECT sum(cityHash64(v, round(finalizeAggregation(s).1, 6)))
FROM (SELECT v, contingencyTupleState(tuple(b), tuple(c)) OVER (PARTITION BY p ORDER BY v) AS s FROM t_window_state_variant_gather ORDER BY v DESC)
SETTINGS make_distributed_plan = 0, query_plan_enable_optimizations = 0, max_bytes_before_external_sort = 0,
    max_bytes_ratio_before_external_sort = 0, max_block_size = 8;

-- query_plan_lower_array_join_function would move the arrayJoin out of the expression into an ArrayJoinStep.
SELECT '-- arrayJoin over the window state, not lowered: distributed then local';
SELECT round(contingencyMerge(CAST(arrayJoin([s]) AS AggregateFunction(contingency, UInt32, UInt32))), 6)
FROM (SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1)
SETTINGS query_plan_lower_array_join_function = 0;
SELECT round(contingencyMerge(CAST(arrayJoin([s]) AS AggregateFunction(contingency, UInt32, UInt32))), 6)
FROM (SELECT contingencyState(b, c) OVER () AS s FROM t_window_state_variant_gather LIMIT 1)
SETTINGS make_distributed_plan = 0, query_plan_lower_array_join_function = 0;

DROP TABLE t_window_state_variant_gather;
