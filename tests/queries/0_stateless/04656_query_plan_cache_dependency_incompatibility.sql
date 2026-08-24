-- Tags: no-parallel
-- Tag no-parallel: resets the global query plan cache and inspects system.query_log.

SET enable_query_plan_cache = 1;
SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS qpc_dependency_incompatible;
DROP TABLE IF EXISTS qpc_dependency_incompatible_test_start;
CREATE TABLE qpc_dependency_incompatible_test_start (ts DateTime64(6)) ENGINE = Memory;
INSERT INTO qpc_dependency_incompatible_test_start VALUES (now64(6));
CREATE TABLE qpc_dependency_incompatible
(
    a UInt64,
    v UInt64,
    d UInt64 DEFAULT a + 1,
    m UInt64 MATERIALIZED a + 2,
    z UInt64 ALIAS a + 3
)
ENGINE = MergeTree
ORDER BY a;
INSERT INTO qpc_dependency_incompatible (a, v) VALUES (1, 100);
SYSTEM DROP QUERY PLAN CACHE;

SELECT tuple(v, d, m, z) FROM qpc_dependency_incompatible ORDER BY a SETTINGS log_comment = 'qpc_incompatible_seed';

ALTER TABLE qpc_dependency_incompatible MODIFY COLUMN z UInt64 ALIAS a + 4;
SELECT tuple(v, d, m, z) FROM qpc_dependency_incompatible ORDER BY a SETTINGS log_comment = 'qpc_incompatible_alias';
SELECT tuple(v, d, m, z) FROM qpc_dependency_incompatible ORDER BY a SETTINGS log_comment = 'qpc_incompatible_alias_hit';

ALTER TABLE qpc_dependency_incompatible MODIFY COLUMN d UInt64 DEFAULT a + 10;
TRUNCATE TABLE qpc_dependency_incompatible;
INSERT INTO qpc_dependency_incompatible (a, v) VALUES (2, 200);
SELECT tuple(v, d, m, z) FROM qpc_dependency_incompatible ORDER BY a SETTINGS log_comment = 'qpc_incompatible_default';

ALTER TABLE qpc_dependency_incompatible MODIFY COLUMN m UInt64 MATERIALIZED a + 20;
TRUNCATE TABLE qpc_dependency_incompatible;
INSERT INTO qpc_dependency_incompatible (a, v) VALUES (2, 200);
SELECT tuple(v, d, m, z) FROM qpc_dependency_incompatible ORDER BY a SETTINGS log_comment = 'qpc_incompatible_materialized';

ALTER TABLE qpc_dependency_incompatible MODIFY COLUMN v UInt32;
SELECT tuple(v, d, m, z) FROM qpc_dependency_incompatible ORDER BY a SETTINGS log_comment = 'qpc_incompatible_type';

DROP TABLE qpc_dependency_incompatible SYNC;
CREATE TABLE qpc_dependency_incompatible
(
    a UInt32,
    v UInt32,
    d UInt64 DEFAULT a + 10,
    m UInt64 MATERIALIZED a + 20,
    z UInt64 ALIAS a + 4
)
ENGINE = ReplacingMergeTree
ORDER BY a;
INSERT INTO qpc_dependency_incompatible (a, v) VALUES (3, 300);
SELECT tuple(v, d, m, z) FROM qpc_dependency_incompatible ORDER BY a SETTINGS log_comment = 'qpc_incompatible_engine';

DROP TABLE qpc_dependency_incompatible SYNC;
CREATE TABLE qpc_dependency_incompatible
(
    a UInt32,
    v UInt32,
    d UInt64 DEFAULT a + 10,
    m UInt64 MATERIALIZED a + 20,
    z UInt64 ALIAS a + 4
)
ENGINE = ReplacingMergeTree
ORDER BY tuple();
INSERT INTO qpc_dependency_incompatible (a, v) VALUES (4, 400);
SELECT tuple(v, d, m, z) FROM qpc_dependency_incompatible ORDER BY a SETTINGS log_comment = 'qpc_incompatible_ordering';

SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['QueryPlanCacheHits'],
    ProfileEvents['QueryPlanCacheMisses'],
    ProfileEvents['QueryPlanCacheValidationMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND event_time_microseconds >= (SELECT ts FROM qpc_dependency_incompatible_test_start)
  AND startsWith(log_comment, 'qpc_incompatible_')
  AND log_comment != 'qpc_incompatible_seed'
ORDER BY log_comment;

DROP TABLE qpc_dependency_incompatible;
DROP TABLE qpc_dependency_incompatible_test_start;
