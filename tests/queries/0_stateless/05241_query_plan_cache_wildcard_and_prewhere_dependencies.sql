-- Tags: no-parallel
-- Tag no-parallel: resets the global query plan cache and inspects system.query_log.

-- A cached plan must be revalidated against every column it depends on: the columns a wildcard
-- expands into, and the columns read only to evaluate an explicit `PREWHERE`.

SET enable_query_plan_cache = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS qpc_wildcard;
DROP TABLE IF EXISTS qpc_prewhere;
DROP TABLE IF EXISTS qpc_05241_test_start;
CREATE TABLE qpc_05241_test_start (ts DateTime64(6)) ENGINE = Memory;
INSERT INTO qpc_05241_test_start VALUES (now64(6));

CREATE TABLE qpc_wildcard (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO qpc_wildcard VALUES (1, 10);
SYSTEM DROP QUERY PLAN CACHE;

SELECT * FROM qpc_wildcard SETTINGS log_comment = 'qpc_05241_star_seed';
SELECT * EXCEPT b FROM qpc_wildcard SETTINGS log_comment = 'qpc_05241_except_seed';
SELECT COLUMNS('^[a-z]$') FROM qpc_wildcard SETTINGS log_comment = 'qpc_05241_columns_seed';
SELECT b FROM qpc_wildcard SETTINGS log_comment = 'qpc_05241_plain_seed';

-- Every wildcard entry must see the new column; the entry without a wildcard stays valid.
ALTER TABLE qpc_wildcard ADD COLUMN c UInt64 DEFAULT 7;
SELECT * FROM qpc_wildcard SETTINGS log_comment = 'qpc_05241_star_after_add';
SELECT * EXCEPT b FROM qpc_wildcard SETTINGS log_comment = 'qpc_05241_except_after_add';
SELECT COLUMNS('^[a-z]$') FROM qpc_wildcard SETTINGS log_comment = 'qpc_05241_columns_after_add';
SELECT b FROM qpc_wildcard SETTINGS log_comment = 'qpc_05241_plain_after_add';

-- The re-seeded entry is reused.
SELECT * FROM qpc_wildcard SETTINGS log_comment = 'qpc_05241_star_hit';

ALTER TABLE qpc_wildcard DROP COLUMN c;
SELECT * FROM qpc_wildcard SETTINGS log_comment = 'qpc_05241_star_after_drop';

CREATE TABLE qpc_prewhere (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY b;
INSERT INTO qpc_prewhere VALUES (1, 10), (2, 20);

SELECT b FROM qpc_prewhere PREWHERE a > 1 SETTINGS log_comment = 'qpc_05241_prewhere_seed';
SELECT b FROM qpc_prewhere PREWHERE a > 1 SETTINGS log_comment = 'qpc_05241_prewhere_hit';

-- `a` is not in the output header of the read step, but the cached `PREWHERE` depends on it.
ALTER TABLE qpc_prewhere MODIFY COLUMN a Int64;
SELECT b FROM qpc_prewhere PREWHERE a > 1 SETTINGS log_comment = 'qpc_05241_prewhere_after_modify';

SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['QueryPlanCacheHits'],
    ProfileEvents['QueryPlanCacheMisses'],
    ProfileEvents['QueryPlanCacheValidationMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND event_time_microseconds >= (SELECT ts FROM qpc_05241_test_start)
  AND startsWith(log_comment, 'qpc_05241_')
  AND NOT endsWith(log_comment, '_seed')
ORDER BY log_comment;

-- The hit path logs the physical read columns, including the one read only for `PREWHERE`.
SELECT log_comment, arraySort(arrayMap(x -> splitByChar('.', x)[-1], columns))
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND event_time_microseconds >= (SELECT ts FROM qpc_05241_test_start)
  AND log_comment IN ('qpc_05241_prewhere_seed', 'qpc_05241_prewhere_hit')
ORDER BY log_comment;

DROP TABLE qpc_wildcard;
DROP TABLE qpc_prewhere;
DROP TABLE qpc_05241_test_start;
