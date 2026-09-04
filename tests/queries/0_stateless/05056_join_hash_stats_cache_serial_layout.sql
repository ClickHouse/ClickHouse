-- Warm `HashTablesStatistics` can still make a hash join go serial.
-- `use_statistics = 0` and `ORDER BY ()` keep the right-table estimate at
-- `totalRows` (and a pushed filter is treated as unknown), so the cold plan is
-- parallel. After a filtered run the cache holds the smaller `source_rows`;
-- the same query then fills with one `FillingRightJoinSide`.
-- Random settings limits: max_threads=(8, 8); parallel_hash_join_threshold=(5001, 5001); collect_hash_table_stats_during_joins=(1, 1); use_hash_table_stats_for_join_reordering=(1, 1); use_statistics=(0, 0); enable_analyzer=(1, 1)

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = false;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET use_query_condition_cache = 0;
SET use_statistics = 0;
SET collect_hash_table_stats_during_joins = 1;
SET use_hash_table_stats_for_join_reordering = 1;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET explain_query_plan_default = 'legacy';
SET join_algorithm = 'hash';
SET parallel_hash_join_threshold = 5001;
SET max_threads = 8;

DROP TABLE IF EXISTS t05056_l;
DROP TABLE IF EXISTS t05056_r;
CREATE TABLE t05056_l (a UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t05056_r (a UInt64) ENGINE = MergeTree ORDER BY ();
INSERT INTO t05056_l SELECT number FROM numbers(20000);
INSERT INTO t05056_r SELECT number FROM numbers(10000);

SELECT 'cold';
SELECT coalesce(
    nullIf(max(toUInt64OrZero(extract(explain, 'FillingRightJoinSide × (\\d+)'))), 0),
    countIf(explain LIKE '%FillingRightJoinSide%'))
FROM (
    EXPLAIN PIPELINE
    SELECT t1.a FROM t05056_l AS t1 INNER JOIN t05056_r AS t2 ON t1.a = t2.a WHERE t2.a < 1000
    SETTINGS max_threads = 8, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = false, enable_analyzer = 1, join_algorithm = 'hash', parallel_hash_join_threshold = 5001, collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1, use_statistics = 0
);

SELECT count() FROM t05056_l AS t1 INNER JOIN t05056_r AS t2 ON t1.a = t2.a WHERE t2.a < 1000
SETTINGS max_threads = 8, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = false, enable_analyzer = 1, join_algorithm = 'hash', parallel_hash_join_threshold = 5001, collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1, use_statistics = 0
FORMAT Null;

SELECT 'warm';
SELECT coalesce(
    nullIf(max(toUInt64OrZero(extract(explain, 'FillingRightJoinSide × (\\d+)'))), 0),
    countIf(explain LIKE '%FillingRightJoinSide%'))
FROM (
    EXPLAIN PIPELINE
    SELECT t1.a FROM t05056_l AS t1 INNER JOIN t05056_r AS t2 ON t1.a = t2.a WHERE t2.a < 1000
    SETTINGS max_threads = 8, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = false, enable_analyzer = 1, join_algorithm = 'hash', parallel_hash_join_threshold = 5001, collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1, use_statistics = 0
);

DROP TABLE t05056_l;
DROP TABLE t05056_r;
