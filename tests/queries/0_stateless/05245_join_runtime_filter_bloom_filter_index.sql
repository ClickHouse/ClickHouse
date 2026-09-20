-- A `bloom_filter` index can test the exact key values of a join runtime filter, but never the
-- `[min, max]` key range the build side can record: `MergeTreeIndexConditionBloomFilter` understands
-- only equality, `IN` and the `has*` functions, so a range predicate is always unknown there. A probe
-- key that only such an index covers is therefore a consumer of the filter, but not of its key range,
-- and the build side must not pay the extra pass over every chunk for it.

SET explain_query_plan_default = 'legacy'; -- the `Key range tracking` line is printed by the non-pretty EXPLAIN
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET join_algorithm = 'hash';
SET max_bytes_ratio_before_external_join = 0;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET max_insert_threads = 1;

DROP TABLE IF EXISTS probe_bloom_only;
DROP TABLE IF EXISTS probe_minmax;
DROP TABLE IF EXISTS build_small;
DROP TABLE IF EXISTS build_large;

CREATE TABLE probe_bloom_only (k UInt64, v UInt64, INDEX idx_v v TYPE bloom_filter GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
CREATE TABLE probe_minmax (k UInt64, v UInt64, INDEX idx_v v TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
CREATE TABLE build_small (v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE build_large (v UInt64) ENGINE = MergeTree ORDER BY v;

INSERT INTO probe_bloom_only SELECT number, number FROM numbers(10000);
INSERT INTO probe_minmax SELECT number, number FROM numbers(10000);
INSERT INTO build_small SELECT number * 1000 FROM numbers(5);
INSERT INTO build_large SELECT number FROM numbers(5000);

-- The probe key is covered by a `bloom_filter` index only: the exact values are exposed (the index can
-- test them), the key range is not tracked (the index could not test it anyway).
SELECT 'bloom_filter index only';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM probe_bloom_only AS p INNER JOIN build_small AS b ON p.v = b.v
) WHERE explain LIKE '%Index analysis%' OR explain LIKE '%Key range tracking%';

-- The control: a `minmax` index can test the range, so it is tracked.
SELECT 'minmax index';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM probe_minmax AS p INNER JOIN build_small AS b ON p.v = b.v
) WHERE explain LIKE '%Index analysis%' OR explain LIKE '%Key range tracking%';

-- Dropping the key range tracking must not drop the pruning: the `bloom_filter` index still gets the
-- exact `IN` set of the filter.
SELECT 'results';
SELECT count(), sum(p.k) FROM probe_bloom_only AS p INNER JOIN build_small AS b ON p.v = b.v
    SETTINGS log_comment = '05245_bloom_only';
SELECT count(), sum(p.k) FROM probe_minmax AS p INNER JOIN build_small AS b ON p.v = b.v
    SETTINGS log_comment = '05245_minmax';

-- The control for the case below: a filter that overflowed `join_runtime_filter_exact_values_limit`
-- keeps no exact values, and the `bloom_filter` index cannot test the recorded range, so alone it
-- prunes nothing.
SELECT count(), sum(p.k) FROM probe_bloom_only AS p INNER JOIN build_large AS b ON p.v = b.v
    SETTINGS join_runtime_filter_exact_values_limit = 1000, log_comment = '05245_bloom_only_overflowed_filter';

-- Two runtime filters on the same probe key: the larger build side overflows
-- `join_runtime_filter_exact_values_limit` and keeps no exact values, the smaller one stays exact and
-- within the `bloom_filter` `IN` cap. The index must be decided by the filter that can still be used,
-- not by whichever descriptor comes first.
SELECT count(), sum(p.k) FROM probe_bloom_only AS p
    INNER JOIN build_large AS b1 ON p.v = b1.v
    INNER JOIN build_small AS b2 ON p.v = b2.v
    SETTINGS join_runtime_filter_exact_values_limit = 1000, log_comment = '05245_bloom_only_two_filters';

SYSTEM FLUSH LOGS query_log;
SELECT 'granules dropped';
SELECT
    log_comment,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('05245_bloom_only', '05245_minmax', '05245_bloom_only_overflowed_filter',
        '05245_bloom_only_two_filters')
    AND type = 'QueryFinish'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE probe_bloom_only;
DROP TABLE probe_minmax;
DROP TABLE build_small;
DROP TABLE build_large;
