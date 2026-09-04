-- Tags: no-random-settings, no-random-merge-tree-settings

-- Oracle: the `AggregationOptimizedEqualRangesOfKeys` assertion. The `count`/`sum` line is
-- not one, it holds with the fix reverted.

DROP TABLE IF EXISTS t_ttl_group_by_consecutive_keys;

CREATE TABLE t_ttl_group_by_consecutive_keys (k UInt64, d DateTime, v UInt64)
ENGINE = MergeTree ORDER BY k
TTL d + INTERVAL 1 SECOND GROUP BY k SET v = sum(v), d = max(d)
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1024, merge_with_ttl_timeout = 100000;

INSERT INTO t_ttl_group_by_consecutive_keys
SELECT number % 100, now() - INTERVAL 1 DAY, number FROM numbers(200000)
SETTINGS max_insert_block_size = 200000;

OPTIMIZE TABLE t_ttl_group_by_consecutive_keys FINAL;
SYSTEM FLUSH LOGS part_log;

SELECT count(), sum(v) FROM t_ttl_group_by_consecutive_keys;

SELECT ProfileEvents['AggregationOptimizedEqualRangesOfKeys'] > 0
FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND database = currentDatabase() AND table = 't_ttl_group_by_consecutive_keys'
    AND event_type = 'MergeParts' AND read_rows = 200000;

DROP TABLE t_ttl_group_by_consecutive_keys;
