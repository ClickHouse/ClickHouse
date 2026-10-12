-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas; breaks GROUP BY correctness and cache ProfileEvents.

-- Partial aggregate cache: disable fail-close when `PREWHERE`/`WHERE` contains subquery.

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_partial_agg_cache_subquery_fact;
DROP TABLE IF EXISTS test_partial_agg_cache_subquery_dim;

CREATE TABLE test_partial_agg_cache_subquery_fact
(
    id UInt32,
    category String,
    value Int64
)
ENGINE = MergeTree()
ORDER BY (id, category);

CREATE TABLE test_partial_agg_cache_subquery_dim
(
    id UInt32
)
ENGINE = MergeTree()
ORDER BY id;

SYSTEM STOP MERGES test_partial_agg_cache_subquery_fact;
SYSTEM STOP MERGES test_partial_agg_cache_subquery_dim;

SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

INSERT INTO test_partial_agg_cache_subquery_fact VALUES
    (1, 'A', 10),
    (1, 'B', 100),
    (2, 'A', 20),
    (2, 'B', 200);

INSERT INTO test_partial_agg_cache_subquery_dim VALUES (1);

SELECT '--- First run with subquery predicate';

SELECT category, sum(value), count()
FROM test_partial_agg_cache_subquery_fact
WHERE id IN (SELECT id FROM test_partial_agg_cache_subquery_dim)
GROUP BY category
ORDER BY category
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_cache_subquery_q1';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] = 0 AS no_hits,
    ProfileEvents['PartialAggregateCacheMisses'] = 0 AS no_misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_subquery_q1'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '--- Change subquery source, result must change';

INSERT INTO test_partial_agg_cache_subquery_dim VALUES (2);

SELECT category, sum(value), count()
FROM test_partial_agg_cache_subquery_fact
WHERE id IN (SELECT id FROM test_partial_agg_cache_subquery_dim)
GROUP BY category
ORDER BY category
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_cache_subquery_q2';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] = 0 AS no_hits,
    ProfileEvents['PartialAggregateCacheMisses'] = 0 AS no_misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_subquery_q2'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE test_partial_agg_cache_subquery_fact;
DROP TABLE test_partial_agg_cache_subquery_dim;
