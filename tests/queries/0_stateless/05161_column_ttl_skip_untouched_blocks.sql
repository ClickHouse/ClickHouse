DROP TABLE IF EXISTS t_ttl_skip_untouched_blocks;

CREATE TABLE t_ttl_skip_untouched_blocks
(
    event_time DateTime,
    id UInt64,
    value String TTL event_time + INTERVAL 1 SECOND
)
ENGINE = MergeTree
ORDER BY (event_time, id)
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    merge_max_block_size = 8192,
    index_granularity = 1,
    merge_with_ttl_timeout = 0;

SYSTEM STOP MERGES t_ttl_skip_untouched_blocks;

INSERT INTO t_ttl_skip_untouched_blocks
SELECT now() - INTERVAL 1 HOUR, number, 'expired' FROM numbers(10000);

INSERT INTO t_ttl_skip_untouched_blocks
SELECT now() + INTERVAL 1 HOUR, number + 10000, 'live' FROM numbers(10000);

SYSTEM START MERGES t_ttl_skip_untouched_blocks;
OPTIMIZE TABLE t_ttl_skip_untouched_blocks FINAL;

SELECT count(), countIf(value = ''), countIf(value = 'live') FROM t_ttl_skip_untouched_blocks;

SELECT
    countIf(column_ttl_min > now()),
    countIf(column_ttl_max > now())
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_ttl_skip_untouched_blocks'
  AND column = 'value'
  AND active;

DROP TABLE t_ttl_skip_untouched_blocks;
