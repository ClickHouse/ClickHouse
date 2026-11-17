-- Multi-key GROUP BY with LowCardinality keys uses serialized aggregation, whose keys must not
-- depend on the dictionary order of the individual parts: the keys are serialized by value.

DROP TABLE IF EXISTS lc_serialized_group_by;
CREATE TABLE lc_serialized_group_by
(
    s LowCardinality(String),
    f LowCardinality(FixedString(4)),
    n LowCardinality(Nullable(String)),
    p Nullable(String),
    k UInt64,
    v UInt64
)
ENGINE = MergeTree ORDER BY k;

-- The second part builds its LowCardinality dictionaries in a different order.
INSERT INTO lc_serialized_group_by SELECT toString(number % 7), toFixedString(toString(number % 5), 4), if(number % 11 = 0, NULL, toString(number % 3)), if(number % 7 = 0, NULL, toString(number % 4)), number, number FROM numbers(3000);
INSERT INTO lc_serialized_group_by SELECT toString((number * 3 + 2) % 7), toFixedString(toString((number * 4 + 1) % 5), 4), if(number % 5 = 0, NULL, toString((number * 2 + 1) % 3)), if(number % 3 = 0, NULL, toString((number + 1) % 5)), number, number FROM numbers(3000);

SELECT s, f, count(), sum(v) FROM lc_serialized_group_by GROUP BY s, f ORDER BY s, f;
SELECT s, f, count(), sum(v) FROM lc_serialized_group_by GROUP BY s, f ORDER BY s, f SETTINGS max_threads = 1, group_by_two_level_threshold = 1;
SELECT s, f, count(), sum(v) FROM lc_serialized_group_by GROUP BY s, f ORDER BY s, f SETTINGS max_block_size = 64;
SELECT s, f, count(), sum(v) FROM lc_serialized_group_by GROUP BY s, f ORDER BY s, f LIMIT 5;
SELECT s, k % 4 AS m, count(), sum(v) FROM lc_serialized_group_by GROUP BY s, m ORDER BY s, m;
SELECT s, n, count() FROM lc_serialized_group_by GROUP BY s, n ORDER BY s, n NULLS FIRST;
SELECT s, p, count(), sum(v) FROM lc_serialized_group_by GROUP BY s, p ORDER BY s, p NULLS FIRST;
SELECT s, p, count(), sum(v) FROM lc_serialized_group_by GROUP BY s, p ORDER BY s, p NULLS FIRST SETTINGS max_block_size = 64, group_by_two_level_threshold = 1;

DROP TABLE IF EXISTS lc_serialized_group_by;

-- A top-K heap can freeze and fall back to ordinary aggregation in the middle of a query. The
-- worst `(s, f)` pair in the sort order is rare and the deliberately small observation window makes
-- the heap freeze after the first block, so the remaining blocks are aggregated without the ranked
-- columns. Pin the settings the path depends on: CI randomizes some of them, and the top-K
-- optimization does not apply to serialized plans.
SET serialize_query_plan = 0;
-- The test server's default profile sets `max_rows_to_group_by` (10G), which disables the GROUP BY
-- top-K optimization; pin it to zero like the other top-K heap tests.
SET max_rows_to_group_by = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_group_by_top_k_optimization = 1;
SET group_by_top_k_optimization_observation_rows = 1;
-- One stream and small blocks, so the heap freezes deterministically at the start of the second block.
SET max_threads = 1;
SET max_block_size = 8192;
SET enable_parallel_replicas = 0;
SET log_queries = 1;

DROP TABLE IF EXISTS lc_serialized_group_by_topk;
CREATE TABLE lc_serialized_group_by_topk
(
    s LowCardinality(String),
    f LowCardinality(FixedString(4)),
    v UInt64
)
ENGINE = MergeTree ORDER BY v;

INSERT INTO lc_serialized_group_by_topk SELECT if(number % 1000 < 995, 'a', 'b'), if(number % 1000 < 500, toFixedString('x', 4), toFixedString('y', 4)), number FROM numbers(200000);

-- The probe discards its output; it exists so that the query_log assertion below can prove that the
-- heap actually froze. The result query below would also pass if the optimization never engaged.
SELECT s, f, count(), sum(v) FROM lc_serialized_group_by_topk GROUP BY s, f ORDER BY s, f LIMIT 2
SETTINGS log_comment = '05241_lc_topk_freeze' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The heap must freeze on the second block, after which the remaining blocks serialize the
-- LowCardinality keys directly instead of materializing the ranked columns.
SELECT max(ProfileEvents['AggregationTopKHeapsFrozen']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND log_comment = '05241_lc_topk_freeze';

-- The results are the same whether or not the heap froze.
SELECT s, f, count(), sum(v) FROM lc_serialized_group_by_topk GROUP BY s, f ORDER BY s, f LIMIT 2 SETTINGS max_threads = 1, max_block_size = 8192, group_by_top_k_optimization_observation_rows = 1;

DROP TABLE IF EXISTS lc_serialized_group_by_topk;
