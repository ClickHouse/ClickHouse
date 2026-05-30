-- Tags: shard

-- Tests the `skip_unavailable_shards_mode` setting on the SELECT path.
-- Shard 2 (of 3) raises an exception via `throwIf` before returning any data; whether the
-- exception is ignored depends on the mode (see the setting description in src/Core/Settings.cpp).

-- `unavailable_or_exception_before_processing`: the exception arrives before any data, so shard 2 is skipped.
SELECT * FROM remote('127.0.0.{1,2,3}', view(SELECT throwIf(shardNum() = 2) AS x, shardNum() AS sn)) ORDER BY sn
SETTINGS prefer_localhost_replica = 0, skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_exception_before_processing';

-- `unavailable` (legacy): the exception is rethrown.
SELECT * FROM remote('127.0.0.{1,2,3}', view(SELECT throwIf(shardNum() = 2) AS x, shardNum() AS sn)) ORDER BY sn
SETTINGS prefer_localhost_replica = 0, skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- `unavailable_or_table_missing`: only a missing table/database is ignored, not an arbitrary exception.
SELECT * FROM remote('127.0.0.{1,2,3}', view(SELECT throwIf(shardNum() = 2) AS x, shardNum() AS sn)) ORDER BY sn
SETTINGS prefer_localhost_replica = 0, skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_table_missing'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- The mode has no effect unless `skip_unavailable_shards` is enabled.
SELECT * FROM remote('127.0.0.{1,2,3}', view(SELECT throwIf(shardNum() = 2) AS x, shardNum() AS sn)) ORDER BY sn
SETTINGS prefer_localhost_replica = 0, skip_unavailable_shards = 0, skip_unavailable_shards_mode = 'unavailable_or_exception_before_processing'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
