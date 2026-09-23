-- The `bloom_filter` cap of `enable_join_runtime_filters_index_analysis` (at most
-- `join_runtime_filter_exact_values_limit / 100` exact values, see `05248_join_runtime_filter_bloom_filter_index_cap`)
-- holds for every runtime filter on its own. A probe key joined twice registers one filter per join, and
-- when one of them is within the cap the `bloom_filter` index is used with a predicate built from the
-- admitted filters only - a sibling filter above the cap stays out of the index condition.
--
-- The join order is pinned, because the build side of the join planned second is itself filtered by the
-- runtime filter of the first one: with `build_under_cap` joined first, the filter of `build_over_cap` is
-- built second from all of its 500 keys (above the cap), and the filter of `build_under_cap` is then
-- built from its 50 keys (within the cap), so the probe read gets one filter on each side of the cap. In
-- the other order both filters keep 50 keys, which covers two admitted filters on one index.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET join_runtime_filter_exact_values_limit = 10000;
SET join_algorithm = 'hash';
SET max_bytes_ratio_before_external_join = 0;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET max_insert_threads = 1;
SET query_plan_optimize_join_order_limit = 1;

DROP TABLE IF EXISTS probe_bloom_only;
DROP TABLE IF EXISTS build_under_cap;
DROP TABLE IF EXISTS build_over_cap;

CREATE TABLE probe_bloom_only (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.001) GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
CREATE TABLE build_under_cap (v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE build_over_cap (v UInt64) ENGINE = MergeTree ORDER BY v;

INSERT INTO probe_bloom_only SELECT number, number FROM numbers(10000);
-- 50 keys: within the cap of 100.
INSERT INTO build_under_cap SELECT number * 100 FROM numbers(50);
-- 500 keys: above the cap of 100, but with the exact values kept. A superset of the keys above.
INSERT INTO build_over_cap SELECT number * 10 FROM numbers(500);

SELECT 'results';
SELECT count(), sum(p.k) FROM probe_bloom_only AS p
    INNER JOIN build_over_cap AS b1 ON p.v = b1.v
    INNER JOIN build_under_cap AS b2 ON p.v = b2.v
    SETTINGS log_comment = '05250_over_then_under';
SELECT count(), sum(p.k) FROM probe_bloom_only AS p
    INNER JOIN build_under_cap AS b1 ON p.v = b1.v
    INNER JOIN build_over_cap AS b2 ON p.v = b2.v
    SETTINGS log_comment = '05250_under_then_over';

SYSTEM FLUSH LOGS query_log;
SELECT 'granules considered and dropped';
SELECT
    log_comment,
    argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time) > 0,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('05250_over_then_under', '05250_under_then_over')
    AND type = 'QueryFinish'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE probe_bloom_only;
DROP TABLE build_under_cap;
DROP TABLE build_over_cap;
