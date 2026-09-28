-- Tags: shard

-- Every read below at `serialize_query_plan = 1` is followed by the same read at 0, so each
-- value is an oracle rather than a snapshot: the two must agree.

DROP TABLE IF EXISTS mrg_local_05059;
DROP TABLE IF EXISTS mrg_dist_05059;
DROP TABLE IF EXISTS mrg_all_05059;
DROP TABLE IF EXISTS mrg_one_05059;

CREATE TABLE mrg_local_05059 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO mrg_local_05059 SELECT number FROM numbers(1000);
CREATE TABLE mrg_dist_05059 (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), mrg_local_05059);

-- Spans a local and a distributed table, so its children fold to a stage above `FetchColumns`.
CREATE TABLE mrg_all_05059 (x UInt64) ENGINE = Merge(currentDatabase(), '^mrg_(local|dist)_05059$');

-- Spans only the distributed table, which reports `Complete` for a single node.
CREATE TABLE mrg_one_05059 (x UInt64) ENGINE = Merge(currentDatabase(), '^mrg_dist_05059$');

-- { echo }
SELECT count() FROM remote('127.0.0.1', currentDatabase(), mrg_all_05059) SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, enable_analyzer = 1;
SELECT count() FROM remote('127.0.0.1', currentDatabase(), mrg_all_05059) SETTINGS serialize_query_plan = 0, prefer_localhost_replica = 0, enable_analyzer = 1;

SELECT sum(x) FROM remote('127.0.0.1', currentDatabase(), mrg_all_05059) SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, enable_analyzer = 1;
SELECT sum(x) FROM remote('127.0.0.1', currentDatabase(), mrg_all_05059) SETTINGS serialize_query_plan = 0, prefer_localhost_replica = 0, enable_analyzer = 1;

SELECT count() FROM remote('127.0.0.1', currentDatabase(), mrg_one_05059) SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, enable_analyzer = 1;
SELECT count() FROM remote('127.0.0.1', currentDatabase(), mrg_one_05059) SETTINGS serialize_query_plan = 0, prefer_localhost_replica = 0, enable_analyzer = 1;
