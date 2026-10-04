-- Tags: no-parallel
-- Tag no-parallel: resets the global query plan cache and inspects system.query_log.

SET enable_query_plan_cache = 1;
SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas = 0;

CREATE TEMPORARY TABLE qpc_key_dependency_test_start (ts DateTime64(6)) ENGINE = Memory;
INSERT INTO qpc_key_dependency_test_start VALUES (now64(6));

DROP TABLE IF EXISTS qpc_key_dependency;
SYSTEM DROP QUERY PLAN CACHE;

CREATE TABLE qpc_key_dependency
(
    id UInt64,
    value UInt64
)
ENGINE = MergeTree
PARTITION BY id % 2
ORDER BY (id, cityHash64(id));

INSERT INTO qpc_key_dependency VALUES (1, 10), (2, 20), (3, 30);

SELECT sum(value)
FROM qpc_key_dependency
WHERE id > 0
SETTINGS log_comment = 'qpc_partition_seed'
FORMAT Null;

DROP TABLE qpc_key_dependency;

CREATE TABLE qpc_key_dependency
(
    id UInt64,
    value UInt64
)
ENGINE = MergeTree
PARTITION BY id % 3
ORDER BY (id, cityHash64(id));

INSERT INTO qpc_key_dependency VALUES (1, 10), (2, 20), (3, 30);

SELECT sum(value)
FROM qpc_key_dependency
WHERE id > 0
SETTINGS log_comment = 'qpc_partition_changed'
FORMAT Null;

DROP TABLE qpc_key_dependency;
SYSTEM DROP QUERY PLAN CACHE;

CREATE TABLE qpc_key_dependency
(
    id UInt64,
    value UInt64
)
ENGINE = MergeTree
ORDER BY (id, cityHash64(id))
SAMPLE BY id;

INSERT INTO qpc_key_dependency VALUES (1, 10), (2, 20), (3, 30);

SELECT sum(value)
FROM qpc_key_dependency SAMPLE 1 / 2
SETTINGS log_comment = 'qpc_sampling_seed'
FORMAT Null;

DROP TABLE qpc_key_dependency;

CREATE TABLE qpc_key_dependency
(
    id UInt64,
    value UInt64
)
ENGINE = MergeTree
ORDER BY (id, cityHash64(id))
SAMPLE BY cityHash64(id);

INSERT INTO qpc_key_dependency VALUES (1, 10), (2, 20), (3, 30);

SELECT sum(value)
FROM qpc_key_dependency SAMPLE 1 / 2
SETTINGS log_comment = 'qpc_sampling_changed'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['QueryPlanCacheHits'],
    ProfileEvents['QueryPlanCacheMisses'],
    ProfileEvents['QueryPlanCacheValidationMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND event_time_microseconds >= (SELECT ts FROM qpc_key_dependency_test_start)
  AND startsWith(log_comment, 'qpc_')
ORDER BY log_comment;

DROP TABLE qpc_key_dependency;
