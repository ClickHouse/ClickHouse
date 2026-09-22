-- A `bloom_filter` skip index tests the exact values of a join runtime filter one by one for every
-- granule, so `ReadFromMergeTree` admits it only while the filter keeps at most
-- `join_runtime_filter_exact_values_limit / 100` exact values (100 with the default limit). This is a
-- stricter gate than "the filter still has exact values": a key that only such an index covers stops
-- being pruned well before the filter overflows the limit. The setting text documents exactly that,
-- and this test pins it, together with the control that the cap (and not an overflow) is what gates.

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

DROP TABLE IF EXISTS probe_bloom_only;
DROP TABLE IF EXISTS probe_minmax;
DROP TABLE IF EXISTS build_under_cap;
DROP TABLE IF EXISTS build_over_cap;

-- The false positive rate is tightened, because the false positives of a `bloom_filter` index add up
-- over the `IN` set: with the default 0.025 and the 500 keys below, every granule tests positive
-- and nothing is dropped (which is exactly why the cap exists). With 0.001 the control below can
-- still discriminate.
CREATE TABLE probe_bloom_only (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter(0.001) GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
CREATE TABLE probe_minmax (k UInt64, v UInt64, INDEX idx_v v TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
CREATE TABLE build_under_cap (v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE build_over_cap (v UInt64) ENGINE = MergeTree ORDER BY v;

INSERT INTO probe_bloom_only SELECT number, number FROM numbers(10000);
INSERT INTO probe_minmax SELECT number, number FROM numbers(10000);
-- 50 keys: within the cap of 100.
INSERT INTO build_under_cap SELECT number * 100 FROM numbers(50);
-- 500 keys: above the cap of 100, but far below `join_runtime_filter_exact_values_limit`, so the
-- filter still keeps the exact values.
INSERT INTO build_over_cap SELECT number * 10 FROM numbers(500);

SELECT 'results';
SELECT count(), sum(p.k) FROM probe_bloom_only AS p INNER JOIN build_under_cap AS b ON p.v = b.v
    SETTINGS log_comment = '05248_bloom_under_cap';
SELECT count(), sum(p.k) FROM probe_bloom_only AS p INNER JOIN build_over_cap AS b ON p.v = b.v
    SETTINGS log_comment = '05248_bloom_over_cap';
-- The control: raising the limit raises the cap to 1000, and the same 500 keys prune again. So it is
-- the cap that gates above, not an overflow of the filter.
SELECT count(), sum(p.k) FROM probe_bloom_only AS p INNER JOIN build_over_cap AS b ON p.v = b.v
    SETTINGS join_runtime_filter_exact_values_limit = 100000, log_comment = '05248_bloom_over_cap_raised_limit';
-- The control: a `minmax` index has no such cap, it takes the `IN` set (or the key range) as is.
SELECT count(), sum(p.k) FROM probe_minmax AS p INNER JOIN build_over_cap AS b ON p.v = b.v
    SETTINGS log_comment = '05248_minmax_over_cap';

SYSTEM FLUSH LOGS query_log;
-- Above the cap the `bloom_filter` index is refused and there is no primary key to prune with, so
-- there is no consumer left: the dynamic predicate must not even be materialized (`considered` = 0).
SELECT 'granules considered and dropped';
SELECT
    log_comment,
    argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time) > 0,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('05248_bloom_under_cap', '05248_bloom_over_cap', '05248_bloom_over_cap_raised_limit',
        '05248_minmax_over_cap')
    AND type = 'QueryFinish'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE probe_bloom_only;
DROP TABLE probe_minmax;
DROP TABLE build_under_cap;
DROP TABLE build_over_cap;
