-- Tags: distributed

-- The `cold_table` setting of the `Distributed` engine: every SELECT reads the cold table on the initiator in
-- addition to the shards, and the rows are merged as if the cold table were one more shard, with `_shard_num` = 0.
-- `test_cluster_two_shards` has two shards that both point at this server, so each hot row is returned twice.

DROP TABLE IF EXISTS cold_hot;
DROP TABLE IF EXISTS cold_cold;
DROP TABLE IF EXISTS cold_dist;
DROP TABLE IF EXISTS cold_dist_one_shard;

CREATE TABLE cold_hot (key UInt32, value String, hot_only UInt8) ENGINE = MergeTree ORDER BY key;
CREATE TABLE cold_cold (key UInt32, value String) ENGINE = MergeTree ORDER BY key;

INSERT INTO cold_hot VALUES (1, 'hot1', 1), (2, 'hot2', 1);
INSERT INTO cold_cold VALUES (1, 'cold1'), (3, 'cold3');

SELECT 'CREATE checks the cold table';
CREATE TABLE cold_dist (key UInt32, value String, hot_only UInt8) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), cold_hot, key) SETTINGS cold_table = 'cold_dist'; -- { serverError INFINITE_LOOP }
CREATE TABLE cold_dist (key UInt32, value String, hot_only UInt8) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), cold_hot, key) SETTINGS cold_table = 'cold_does_not_exist'; -- { serverError UNKNOWN_TABLE }

CREATE TABLE cold_dist (key UInt32, value String, hot_only UInt8) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), cold_hot, key) SETTINGS cold_table = 'cold_cold';
CREATE TABLE cold_dist_one_shard (key UInt32, value String, hot_only UInt8) ENGINE = Distributed(test_shard_localhost, currentDatabase(), cold_hot) SETTINGS cold_table = 'cold_cold';

SELECT 'rows and _shard_num';
SELECT _shard_num, key, value FROM cold_dist ORDER BY _shard_num, key, value;

SELECT 'aggregation is merged across the shards and the cold table';
SELECT key, count(), uniqExact(value) FROM cold_dist GROUP BY key ORDER BY key;
SELECT count(), sum(key) FROM cold_dist;

SELECT 'sort and limit are completed on the initiator';
SELECT key, value FROM cold_dist ORDER BY key DESC, value LIMIT 2;
SELECT key, value FROM cold_dist ORDER BY key, value LIMIT 1 OFFSET 2;
SELECT key, count() FROM cold_dist GROUP BY key ORDER BY count() DESC, key LIMIT 1;

SELECT 'single shard still merges with the cold table';
SELECT key, count() FROM cold_dist_one_shard GROUP BY key ORDER BY key;
SELECT count() FROM cold_dist_one_shard;
SELECT _shard_num, key FROM cold_dist_one_shard ORDER BY _shard_num, key;

SELECT 'group by the sharding key is still merged';
SELECT key, count() FROM cold_dist GROUP BY key ORDER BY key SETTINGS optimize_distributed_group_by_sharding_key = 1;
SELECT DISTINCT key FROM cold_dist ORDER BY key SETTINGS optimize_distributed_group_by_sharding_key = 1;

SELECT 'skipping unused shards keeps the cold table';
SELECT _shard_num, key FROM cold_dist WHERE key = 1 ORDER BY _shard_num SETTINGS optimize_skip_unused_shards = 1;
SELECT _shard_num, key FROM cold_dist WHERE key = 3 ORDER BY _shard_num SETTINGS optimize_skip_unused_shards = 1;
SELECT count() FROM cold_dist WHERE key = 1 AND key = 2 SETTINGS optimize_skip_unused_shards = 1;

SELECT 'filters and expressions';
SELECT key, upper(value) FROM cold_dist WHERE value LIKE 'cold%' ORDER BY key;
SELECT count() FROM cold_dist WHERE key IN (SELECT key FROM cold_cold);

SELECT 'a column missing on the cold side fails only when used';
SELECT count() FROM cold_dist WHERE hot_only = 1; -- { serverError UNKNOWN_IDENTIFIER }
SELECT count() FROM cold_dist WHERE key = 2;

SELECT 'inserts go to the shards only';
INSERT INTO cold_dist SETTINGS distributed_foreground_insert = 1 VALUES (4, 'hot4', 1);
SELECT count() FROM cold_hot;
SELECT count() FROM cold_cold;
DELETE FROM cold_hot WHERE key = 4;

SELECT 'the cold table is resolved at query time';
DROP TABLE cold_cold;
SELECT count() FROM cold_dist; -- { serverError UNKNOWN_TABLE }
DETACH TABLE cold_dist;
ATTACH TABLE cold_dist;
SELECT count() FROM cold_dist; -- { serverError UNKNOWN_TABLE }
CREATE TABLE cold_cold (key UInt32, value String) ENGINE = MergeTree ORDER BY key;
INSERT INTO cold_cold VALUES (5, 'cold5');
SELECT _shard_num, key, value FROM cold_dist ORDER BY _shard_num, key;

DROP TABLE cold_dist;
DROP TABLE cold_dist_one_shard;
DROP TABLE cold_cold;
DROP TABLE cold_hot;
