-- Tags: no-random-merge-tree-settings

DROP TABLE IF EXISTS t_ttl_skip_untouched_blocks;

CREATE TABLE t_ttl_skip_untouched_blocks
(
    event_time DateTime,
    id UInt64,
    value String TTL event_time + INTERVAL 1 SECOND
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    merge_max_block_size = 1024,
    vertical_merge_algorithm_min_rows_to_activate = 1000000000,
    index_granularity = 1,
    merge_with_ttl_timeout = 0;

SYSTEM STOP MERGES t_ttl_skip_untouched_blocks;

INSERT INTO t_ttl_skip_untouched_blocks
SELECT now() + INTERVAL 1 HOUR, number, 'live' FROM numbers(1536)
SETTINGS max_threads = 1, max_insert_threads = 1;

INSERT INTO t_ttl_skip_untouched_blocks
SELECT now() - INTERVAL 1 HOUR, number + 1536, 'expired' FROM numbers(1536)
SETTINGS max_threads = 1, max_insert_threads = 1;

OPTIMIZE TABLE t_ttl_skip_untouched_blocks FINAL SETTINGS optimize_throw_if_noop = 1;
SYSTEM START MERGES t_ttl_skip_untouched_blocks;

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
