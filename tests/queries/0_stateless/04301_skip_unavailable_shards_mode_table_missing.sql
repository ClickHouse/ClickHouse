-- Tags: no-parallel, no-flaky-check, shard

-- Tests the `skip_unavailable_shards_mode` setting on the INSERT path.
-- The underlying table exists only on shard_0; it is missing on shard_1.
-- A foreground (synchronous) distributed INSERT routes the rows to the shards, where the missing
-- table on shard_1 surfaces as an `UNKNOWN_TABLE` exception before any data is processed.

DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;
CREATE DATABASE shard_0;
CREATE DATABASE shard_1;

CREATE TABLE shard_0.t_sus (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO shard_0.t_sus VALUES (10), (20);

CREATE TABLE dist_sus (x UInt32) ENGINE = Distributed('test_cluster_two_shards_different_databases', '', t_sus, x);

-- `unavailable_or_table_missing`: shard_1 (missing table) is skipped; rows routed to shard_0 are inserted.
-- numbers(4) shards by `x % 2`: rows 0 and 2 go to shard_0, rows 1 and 3 to the skipped shard_1.
INSERT INTO dist_sus SELECT number FROM numbers(4)
SETTINGS distributed_foreground_insert = 1, skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_table_missing';

SELECT x FROM shard_0.t_sus ORDER BY x;

-- `unavailable_or_exception_before_processing` also skips the shard whose table is missing.
INSERT INTO dist_sus SELECT number + 100 FROM numbers(4)
SETTINGS distributed_foreground_insert = 1, skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_exception_before_processing';

SELECT x FROM shard_0.t_sus ORDER BY x;

-- `unavailable` (legacy): the missing table is an error.
INSERT INTO dist_sus SELECT number FROM numbers(4)
SETTINGS distributed_foreground_insert = 1, skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable'; -- { serverError UNKNOWN_TABLE }

DROP TABLE dist_sus;
DROP DATABASE shard_0;
DROP DATABASE shard_1;
