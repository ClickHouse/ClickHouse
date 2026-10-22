-- `ADD INDEX IF NOT EXISTS` of an existing `jsonbf_v1` index is a no-op, so it does not need the experimental setting.
SET allow_experimental_json_bloom_filter_index = 1;
DROP TABLE IF EXISTS json_bf_if_not_exists;
CREATE TABLE json_bf_if_not_exists (id UInt64, j JSON, INDEX bf j TYPE jsonbf_v1 GRANULARITY 1) ENGINE = MergeTree ORDER BY id;

SET allow_experimental_json_bloom_filter_index = 0;
ALTER TABLE json_bf_if_not_exists ADD INDEX IF NOT EXISTS bf j TYPE jsonbf_v1 GRANULARITY 1;
ALTER TABLE json_bf_if_not_exists ADD INDEX IF NOT EXISTS bf id TYPE minmax GRANULARITY 1;
-- The index is added when it does not exist, or when it is dropped earlier in the same `ALTER`.
ALTER TABLE json_bf_if_not_exists ADD INDEX IF NOT EXISTS other j TYPE jsonbf_v1 GRANULARITY 1; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE json_bf_if_not_exists DROP INDEX bf, ADD INDEX IF NOT EXISTS bf j TYPE jsonbf_v1 GRANULARITY 1; -- { serverError SUPPORT_IS_DISABLED }
SELECT name, type FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'json_bf_if_not_exists';
DROP TABLE json_bf_if_not_exists;
