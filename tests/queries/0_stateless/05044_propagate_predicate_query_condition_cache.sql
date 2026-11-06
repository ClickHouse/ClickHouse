-- Tags: no-parallel
-- Tag no-parallel: a sibling test's SYSTEM DROP QUERY CONDITION CACHE can evict the entry

-- The copy merges into the target's filter, rebuilding the `FilterStep`: without the cache key the
-- second run cannot reuse the entry. `Memory` source, so only the target read can populate it
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET use_query_condition_cache = 1;

DROP TABLE IF EXISTS prop_qcc_src;
DROP TABLE IF EXISTS prop_qcc_dst;

CREATE TABLE prop_qcc_src (k UInt64) ENGINE = Memory;
CREATE TABLE prop_qcc_dst (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;

INSERT INTO prop_qcc_src SELECT number FROM numbers(100000);
INSERT INTO prop_qcc_dst SELECT number, toString(number) FROM numbers(100000);

SELECT count()
FROM (SELECT * FROM prop_qcc_src WHERE k = 12345) AS s
INNER JOIN prop_qcc_dst AS d ON s.k = d.k
WHERE d.payload != ''
SETTINGS log_comment = 'prop_qcc_populate' FORMAT Null;

SELECT count()
FROM (SELECT * FROM prop_qcc_src WHERE k = 12345) AS s
INNER JOIN prop_qcc_dst AS d ON s.k = d.k
WHERE d.payload != ''
SETTINGS log_comment = 'prop_qcc_reuse' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'second run hits the cache', ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'prop_qcc_reuse'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE prop_qcc_src;
DROP TABLE prop_qcc_dst;
