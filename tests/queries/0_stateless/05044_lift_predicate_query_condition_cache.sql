-- Tags: no-parallel
-- Tag no-parallel: the check asserts a QueryConditionCacheHits on the instance-wide cache, and a
-- sibling test's SYSTEM DROP QUERY CONDITION CACHE can evict the entry between the two runs.

-- The lifted predicate is merged into the target's own filter, which rebuilds the `FilterStep`.
-- If the query condition cache key is not restored, the second run cannot reuse the entry.
-- The source table is `Memory`, so only the target read can populate the cache
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET use_query_condition_cache = 1;

DROP TABLE IF EXISTS lift_qcc_src;
DROP TABLE IF EXISTS lift_qcc_dst;

CREATE TABLE lift_qcc_src (k UInt64) ENGINE = Memory;
CREATE TABLE lift_qcc_dst (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;

INSERT INTO lift_qcc_src SELECT number FROM numbers(100000);
INSERT INTO lift_qcc_dst SELECT number, toString(number) FROM numbers(100000);

SELECT count()
FROM (SELECT * FROM lift_qcc_src WHERE k = 12345) AS s
INNER JOIN lift_qcc_dst AS d ON s.k = d.k
WHERE d.payload != ''
SETTINGS log_comment = 'lift_qcc_populate' FORMAT Null;

SELECT count()
FROM (SELECT * FROM lift_qcc_src WHERE k = 12345) AS s
INNER JOIN lift_qcc_dst AS d ON s.k = d.k
WHERE d.payload != ''
SETTINGS log_comment = 'lift_qcc_reuse' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'second run hits the cache', ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'lift_qcc_reuse'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE lift_qcc_src;
DROP TABLE lift_qcc_dst;
