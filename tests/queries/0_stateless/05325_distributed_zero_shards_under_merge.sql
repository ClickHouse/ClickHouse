-- A `Distributed` table whose shards are all skipped, read through `Merge` by another `Distributed` query, returns the empty-set aggregate.

DROP TABLE IF EXISTS zero_shards_under_merge;
DROP TABLE IF EXISTS zero_shards_under_merge_dist;
DROP TABLE IF EXISTS zero_shards_under_merge_local;

CREATE TABLE zero_shards_under_merge_local (x UInt8) ENGINE = MergeTree ORDER BY x;
CREATE TABLE zero_shards_under_merge_dist (x UInt8) ENGINE = Remote('127.0.0.1,localhost', currentDatabase(), zero_shards_under_merge_local, x);
CREATE TABLE zero_shards_under_merge (x UInt8) ENGINE = Merge(currentDatabase(), '^zero_shards_under_merge_dist$');

INSERT INTO zero_shards_under_merge_local VALUES (7);

SET optimize_skip_unused_shards = 1;

-- { echoOn }
SELECT count(), sum(x) FROM remote('127.0.0.1,localhost', currentDatabase(), zero_shards_under_merge) WHERE x = 1 AND x = 2 SETTINGS force_optimize_skip_unused_shards = 1, serialize_query_plan = 0;
SELECT count(), sum(x) FROM remote('127.0.0.1,localhost', currentDatabase(), zero_shards_under_merge) WHERE 0 SETTINGS force_optimize_skip_unused_shards = 1, serialize_query_plan = 0;
SELECT count(), sum(x) FROM remote('127.0.0.1,localhost', currentDatabase(), zero_shards_under_merge) WHERE x = 7;
-- { echoOff }

DROP TABLE zero_shards_under_merge;
DROP TABLE zero_shards_under_merge_dist;
DROP TABLE zero_shards_under_merge_local;
