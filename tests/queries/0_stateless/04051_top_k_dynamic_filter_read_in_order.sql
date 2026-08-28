-- Regression test: TopK dynamic filtering must be disabled when read-in-order
-- optimization applies on a MergeTree sorting-key prefix.
-- Otherwise the prewhere filter rejects all rows past the threshold in sorted
-- order, preventing early LIMIT cancellation and causing a full table scan.

DROP TABLE IF EXISTS t_topk_rio;

CREATE TABLE t_topk_rio (x UInt64, y String)
ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 64;

INSERT INTO t_topk_rio SELECT number, toString(number) FROM numbers(200000);

-- Correctness: results must match regardless of dynamic filtering.
SELECT x FROM t_topk_rio ORDER BY x LIMIT 5
SETTINGS optimize_read_in_order = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100;

-- The query plan should NOT contain __topKFilter when read-in-order is active
-- on the sorting key prefix.
SELECT explain LIKE '%__topKFilter%' AS has_topk_filter
FROM (
    EXPLAIN actions = 1
    SELECT x FROM t_topk_rio ORDER BY x LIMIT 5
    SETTINGS optimize_read_in_order = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100
)
WHERE has_topk_filter;

-- Verify that read_rows is bounded (not a full scan).
-- With read-in-order + LIMIT 5, we should read far fewer rows than the table contains.
SYSTEM FLUSH LOGS query_log;

SELECT read_rows < 100000
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%SELECT x FROM t_topk_rio ORDER BY x LIMIT 5%'
    AND query NOT LIKE '%system.query_log%'
    AND query NOT LIKE '%EXPLAIN%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Sanity check: when sorting by a non-key column, dynamic filtering should
-- still be applied (the plan should contain `__topKFilter`). Column `y` is
-- `String`, so dynamic filtering for variable-length types must be enabled
-- explicitly via `use_top_k_dynamic_filtering_for_variable_length_types`.
SELECT count() > 0 AS has_topk_filter
FROM (
    EXPLAIN actions = 1
    SELECT y FROM t_topk_rio ORDER BY y LIMIT 5
    SETTINGS optimize_read_in_order = 1, use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 1, query_plan_max_limit_for_top_k_optimization = 100
)
WHERE explain LIKE '%__topKFilter%';

DROP TABLE t_topk_rio;
