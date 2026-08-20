DROP TABLE IF EXISTS t_lwu_condition_cache;

SET use_query_condition_cache = 1;
SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;

CREATE TABLE t_lwu_condition_cache
(
    id UInt64 DEFAULT generateSnowflakeID(),
    exists UInt8
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_condition_cache (exists) SELECT 0 FROM numbers(100000);

SELECT count() FROM t_lwu_condition_cache WHERE exists;

UPDATE t_lwu_condition_cache SET exists = 1 WHERE 1;

SELECT count() FROM t_lwu_condition_cache WHERE exists;

DROP TABLE IF EXISTS t_lwu_condition_cache;
