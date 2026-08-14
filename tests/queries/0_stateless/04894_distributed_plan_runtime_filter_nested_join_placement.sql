-- Tags: no-old-analyzer

CREATE TABLE fact (k1 UInt64, k2 UInt64, v UInt64) ENGINE = MergeTree ORDER BY k1;
CREATE TABLE dim1 (d1 UInt64) ENGINE = MergeTree ORDER BY d1;
CREATE TABLE dim2 (d2 UInt64, attr UInt64) ENGINE = MergeTree ORDER BY d2;
INSERT INTO fact SELECT number, number, number FROM numbers(1000000);
INSERT INTO dim1 SELECT number * 10 FROM numbers(10000);
INSERT INTO dim2 SELECT number * 100, number FROM numbers(1000);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET explain_query_plan_default = 'legacy';
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
-- The plan shape and transported-filter admission depend on the join order and the estimate
-- source, so pin them against test-level randomization.
SET query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_limit = 10, use_hash_table_stats_for_join_reordering = 0, use_statistics = 0;
SET distributed_plan_join_runtime_filters = 1;

-- The apply sites for both dimension filters sit in the fact scan fragment, below BOTH shuffle
-- exchanges of the nested joins, so the receives must be delivered there and not one exchange
-- below the owning join.
SELECT '-- nested joins: receives go to the fact scan fragment';
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID') FROM (
    EXPLAIN SELECT count() FROM fact INNER JOIN dim1 ON fact.k1 = dim1.d1 INNER JOIN dim2 ON fact.k2 = dim2.d2 WHERE dim2.attr < 100
) WHERE explain LIKE '%RuntimeFilter%' OR explain LIKE '%Exchange%' OR explain LIKE '%ReadFromMergeTree%';

SELECT count() FROM fact INNER JOIN dim1 ON fact.k1 = dim1.d1 INNER JOIN dim2 ON fact.k2 = dim2.d2 WHERE dim2.attr < 100;
SELECT count() FROM fact INNER JOIN dim1 ON fact.k1 = dim1.d1 INNER JOIN dim2 ON fact.k2 = dim2.d2 WHERE dim2.attr < 100
    SETTINGS distributed_plan_join_runtime_filters = 0;
