-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A window aggregate of the CrossTab family produces its state in the Window representation, and no
-- header carries the representation. Below a sorted gather the local in-memory exchange handed such
-- states to a consumer whose header named the Aggregation representation, and reading the one layout
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
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
    distributed_plan_default_shuffle_join_bucket_count = 8, distributed_plan_default_reader_bucket_count = 8,
    optimize_read_in_order = 0, optimize_sorting_by_input_stream_properties = 1,
    distributed_plan_optimize_exchanges = 1, max_threads = 8, max_rows_to_group_by = 0;

-- The states must cross the exchange below a SORTED gather: that is the shape which puts a
-- MergingSortedTransform, built from the consumer's header, over what the exchange delivers. Assert it,
-- otherwise a plan change would leave this file passing while covering nothing. The checking query runs
-- non-distributed, because an aggregating query over EXPLAIN would itself be distributed.
SELECT 'sorted gather above the window step: ',
    minIf(n, explain LIKE '%GatherExchange (sorted by%') < minIf(n, explain LIKE '%Window (Window step%')
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

-- theilsU is the negative control: its window state keeps the ordinary state's leading members, so
-- reading it as the ordinary state never crashed and it must keep working either way.
SELECT '-- theilsU control: distributed then local';
SELECT sum(cityHash64(p, v, round(finalizeAggregation(s), 6)))
FROM (SELECT p, v, theilsUState(b, c) OVER (PARTITION BY p ORDER BY v) AS s FROM t_window_state_variant_gather);
SELECT sum(cityHash64(p, v, round(finalizeAggregation(s), 6)))
FROM (SELECT p, v, theilsUState(b, c) OVER (PARTITION BY p ORDER BY v) AS s FROM t_window_state_variant_gather)
SETTINGS make_distributed_plan = 0;

DROP TABLE t_window_state_variant_gather;
