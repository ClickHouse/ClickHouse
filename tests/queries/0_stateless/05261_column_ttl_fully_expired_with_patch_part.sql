-- A merge that applies a patch part does not drop a column fully expired by TTL as a whole, because the patch
-- may change the values of the column. With `ttl_only_drop_columns`, such a merge must still clear the expired
-- values by rewriting the part instead of keeping them.

SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_ttl_expired_patch;

CREATE TABLE t_ttl_expired_patch
(
    d Date,
    key UInt64,
    value String TTL d + INTERVAL 1 DAY
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, ttl_only_drop_columns = 1,
    max_number_of_merges_with_ttl_in_pool = 0, enable_block_number_column = 1, enable_block_offset_column = 1,
    apply_patches_on_merge = 1;

SYSTEM STOP MERGES t_ttl_expired_patch;

INSERT INTO t_ttl_expired_patch VALUES ('2020-01-01', 1, 'expired'), ('2020-01-01', 2, 'expired');
UPDATE t_ttl_expired_patch SET value = 'updated' WHERE key = 1;

SYSTEM START MERGES t_ttl_expired_patch;

OPTIMIZE TABLE t_ttl_expired_patch FINAL;
SELECT key, value FROM t_ttl_expired_patch ORDER BY key;
CHECK TABLE t_ttl_expired_patch SETTINGS check_query_single_value_result = 1;

DROP TABLE t_ttl_expired_patch;
