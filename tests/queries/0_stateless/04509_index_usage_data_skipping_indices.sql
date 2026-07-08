-- Tags: no-parallel-replicas
-- Usage counters count reading steps per server; parallel replicas would multiply them.

DROP TABLE IF EXISTS t_index_usage;

CREATE TABLE t_index_usage
(
    key UInt64,
    v1 UInt64,
    v2 String,
    INDEX idx_v1 v1 TYPE minmax GRANULARITY 1,
    INDEX idx_v2 v2 TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_index_usage SELECT number, number, toString(number) FROM numbers(32);

SELECT 'before queries';
SELECT name, times_evaluated, granules_evaluated, granules_dropped, last_used_time
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_index_usage' ORDER BY name;

-- EXPLAIN analyzes indexes but must not bump the usage counters.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_index_usage WHERE v1 = 1);

SELECT 'after EXPLAIN';
SELECT name, times_evaluated, granules_evaluated, granules_dropped, last_used_time
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_index_usage' ORDER BY name;

SELECT count() FROM t_index_usage WHERE v1 = 1
SETTINGS use_skip_indexes = 1, use_query_condition_cache = 0;

SELECT 'after query using idx_v1';
SELECT name, times_evaluated, granules_evaluated > 0, granules_dropped > 0, last_used_time IS NOT NULL
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_index_usage' ORDER BY name;

SELECT count() FROM t_index_usage WHERE v2 = '2'
SETTINGS use_skip_indexes = 1, use_query_condition_cache = 0;

SELECT 'after query using idx_v2';
SELECT name, times_evaluated, granules_evaluated > 0, granules_dropped > 0, last_used_time IS NOT NULL
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_index_usage' ORDER BY name;

DROP TABLE t_index_usage;
