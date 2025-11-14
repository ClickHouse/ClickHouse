-- Tags: no-fasttest

SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;
SET log_queries = 1;
SET log_query_settings = 1;
SET mutations_sync = 2;
SET max_execution_time = 60;

-- test rely on local execution, - force parallel replicas to genearate local plan
SET parallel_replicas_local_plan=1;

DROP TABLE IF EXISTS sc_core SYNC;

CREATE TABLE sc_core
(
    k UInt32,
    v Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS refresh_statistics_interval = 0;

INSERT INTO sc_core
SELECT number, if(number % 20 = 0, NULL, toFloat64(rand()) / 4294967296.0)
FROM numbers(60000);

ALTER TABLE sc_core ADD STATISTICS v TYPE TDigest;
ALTER TABLE sc_core MATERIALIZE STATISTICS ALL;

------------------------------------------------------------
-- SUM() must not trigger statistics
------------------------------------------------------------
DROP TABLE IF EXISTS sc_unused SYNC;

CREATE TABLE sc_unused
(
    k   UInt64,
    val UInt64
)
ENGINE = MergeTree
ORDER BY k
SETTINGS refresh_statistics_interval = 0;

INSERT INTO sc_unused
SELECT number, number % 100
FROM numbers(50000);

ALTER TABLE sc_unused ADD STATISTICS val TYPE MinMax;
ALTER TABLE sc_unused MATERIALIZE STATISTICS ALL;

SELECT sum(val) FROM sc_unused
SETTINGS use_statistics_cache = 0, log_comment = 'nouse-agg' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT toUInt8(ProfileEvents['LoadedStatisticsMicroseconds'] = 0)
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'nouse-agg'
ORDER BY event_time_microseconds DESC
LIMIT 1;

------------------------------------------------------------
-- LowCardinality: CountMin https://github.com/ClickHouse/ClickHouse/issues/87886
------------------------------------------------------------
DROP TABLE IF EXISTS st_cm_lc SYNC;

CREATE TABLE st_cm_lc
(
    k   UInt32,
    cat LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS refresh_statistics_interval = 0;

INSERT INTO st_cm_lc
SELECT number,
       if(number % 4 = 0, 'PROMO', concat('X', toString(number % 1000)))
FROM numbers(60000);

ALTER TABLE st_cm_lc ADD STATISTICS cat TYPE CountMin;
ALTER TABLE st_cm_lc MATERIALIZE STATISTICS ALL;

SELECT count() FROM st_cm_lc WHERE cat = 'PROMO'
SETTINGS use_statistics_cache = 0, log_comment = 'cm-lc-load' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT toUInt8(ProfileEvents['LoadedStatisticsMicroseconds'] > 0)
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'cm-lc-load'
ORDER BY event_time_microseconds DESC
LIMIT 1;

------------------------------------------------------------
-- JOIN with Uniq
------------------------------------------------------------
DROP TABLE IF EXISTS sj_a SYNC;
DROP TABLE IF EXISTS sj_b SYNC;

CREATE TABLE sj_a (id UInt32, p UInt8)
ENGINE = MergeTree
ORDER BY id
SETTINGS refresh_statistics_interval = 0;

CREATE TABLE sj_b (id UInt32, t LowCardinality(String))
ENGINE = MergeTree
ORDER BY id
SETTINGS refresh_statistics_interval = 0;

INSERT INTO sj_a SELECT number, number % 2 FROM numbers(60000);
INSERT INTO sj_b SELECT number, if(number % 5 = 0, 'PROMO', 'OTHER') FROM numbers(60000);

ALTER TABLE sj_a ADD STATISTICS id TYPE Uniq;
ALTER TABLE sj_b ADD STATISTICS id TYPE Uniq;

ALTER TABLE sj_a MATERIALIZE STATISTICS ALL;
ALTER TABLE sj_b MATERIALIZE STATISTICS ALL;

SELECT count()
FROM sj_a a
JOIN sj_b b ON a.id = b.id
WHERE b.t = 'PROMO'
SETTINGS use_statistics_cache = 0, query_plan_optimize_join_order_limit = 10, log_comment = 'join-load'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT toUInt8(ProfileEvents['LoadedStatisticsMicroseconds'] > 0)
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'join-load'
ORDER BY event_time_microseconds DESC
LIMIT 1;
