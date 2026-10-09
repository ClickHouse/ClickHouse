-- Tags: shard, no-parallel, no-fasttest
-- Test: ABR engine must report read progress to the initiator while it buffers,
-- or hedged requests time out and the initiator spuriously switches to another replica
--
-- Uses test_cluster_one_shard_two_replicas so hedged requests have a second replica to switch to
-- 1 shard, replicas 127.0.0.1 / 127.0.0.2, both the local server

SET send_logs_level = 'error';

DROP TABLE IF EXISTS dist_abr_slow;
DROP TABLE IF EXISTS dist_mt_slow;
DROP TABLE IF EXISTS abr_slow;
DROP TABLE IF EXISTS mt_slow;
DROP TABLE IF EXISTS slow_1;
DROP TABLE IF EXISTS slow_10;
DROP TABLE IF EXISTS slow_100;

-- Underlying MergeTree tiers for the ABR table.
CREATE TABLE slow_1 (
    _sample_interval UInt64 DEFAULT 1,
    namespace UInt64,
    timestamp DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toStartOfMonth(timestamp)
ORDER BY (namespace, timestamp);

CREATE TABLE slow_10 (
    _sample_interval UInt64 DEFAULT 10,
    namespace UInt64,
    timestamp DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toStartOfMonth(timestamp)
ORDER BY (namespace, timestamp);

CREATE TABLE slow_100 (
    _sample_interval UInt64 DEFAULT 100,
    namespace UInt64,
    timestamp DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toStartOfMonth(timestamp)
ORDER BY (namespace, timestamp);

-- Keep the finest tier tiny (4 rows) so sleepEachRow time is bounded and deterministic:
-- 4 rows * 2.0s = 8.0s of silent buffering, comfortably above the 3s receive_data_timeout_ms used
INSERT INTO slow_1
SELECT 1, 200, toDateTime('2026-01-01 00:00:00') - INTERVAL number HOUR, number
FROM system.numbers LIMIT 4;

-- Coarser tiers only need to exist
INSERT INTO slow_10
SELECT 10, 200, toDateTime('2026-01-01 00:00:00') - INTERVAL (10 * number) HOUR, number
FROM system.numbers LIMIT 4;

INSERT INTO slow_100
SELECT 100, 200, toDateTime('2026-01-01 00:00:00') - INTERVAL (100 * number) HOUR, number
FROM system.numbers LIMIT 4;

CREATE TABLE abr_slow
ENGINE = ABR('slow', [1, 10, 100], ('timestamp'));

-- Plain MergeTree control with the SAME 4 rows as the ABR finest tier.
CREATE TABLE mt_slow (
    _sample_interval UInt64 DEFAULT 1,
    namespace UInt64,
    timestamp DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toStartOfMonth(timestamp)
ORDER BY (namespace, timestamp);

INSERT INTO mt_slow
SELECT 1, 200, toDateTime('2026-01-01 00:00:00') - INTERVAL number HOUR, number
FROM system.numbers LIMIT 4;

CREATE TABLE dist_abr_slow AS abr_slow
ENGINE = Distributed('test_cluster_one_shard_two_replicas', currentDatabase(), 'abr_slow', rand());

CREATE TABLE dist_mt_slow AS mt_slow
ENGINE = Distributed('test_cluster_one_shard_two_replicas', currentDatabase(), 'mt_slow', rand());


-- ABR: initiator should NOT switch replicas if progress is reported
-- will fail downstream if no progress reported
SELECT 'ABR Count';
SELECT count() FROM dist_abr_slow
WHERE timestamp >= toDateTime('2026-01-01 00:00:00') - INTERVAL 3 HOUR
  AND sleepEachRow(2) = 0
SETTINGS
  prefer_localhost_replica = 0,
  use_hedged_requests = 1,
  receive_data_timeout_ms = 3000,
  function_sleep_max_microseconds_per_block = 60000000,
  log_comment = '03595_abr_slow_case';

-- Control: identical slow scan over a plain MergeTree behind Distributed.
-- a normal shard streams progress while reading, so no replica change.
SELECT 'MergeTree Control Count';
SELECT count() FROM dist_mt_slow
WHERE timestamp >= toDateTime('2026-01-01 00:00:00') - INTERVAL 3 HOUR
  AND sleepEachRow(2) = 0
SETTINGS
  prefer_localhost_replica = 0,
  use_hedged_requests = 1,
  receive_data_timeout_ms = 3000,
  function_sleep_max_microseconds_per_block = 60000000,
  log_comment = '03595_mt_control_case';

SYSTEM FLUSH LOGS query_log;

-- HedgedRequestsChangeReplica is an initiator-side ProfileEvent
-- Expected no replica change in both cases
SELECT sum(ProfileEvents['HedgedRequestsChangeReplica']) = 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND is_initial_query
  AND log_comment = '03595_abr_slow_case';

SELECT sum(ProfileEvents['HedgedRequestsChangeReplica']) = 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND is_initial_query
  AND log_comment = '03595_mt_control_case';

DROP TABLE IF EXISTS dist_abr_slow;
DROP TABLE IF EXISTS dist_mt_slow;
DROP TABLE IF EXISTS abr_slow;
DROP TABLE IF EXISTS mt_slow;
DROP TABLE IF EXISTS slow_1;
DROP TABLE IF EXISTS slow_10;
DROP TABLE IF EXISTS slow_100;
