-- Rows that a DELETE TTL removes never reach the GROUP BY TTL rule, and the merged part comes out
-- empty. Such a part must not keep the source parts' GROUP BY bound: that bound is expired, so
-- every later TTL selection would pick the empty part up again and merge it forever.

DROP TABLE IF EXISTS t_ttl_group_by_dropped;
DROP TABLE IF EXISTS t_ttl_group_by_where_dropped;

SELECT '-- an unconditional rows TTL drops the whole block before the rule runs';

CREATE TABLE t_ttl_group_by_dropped (id UInt32, ts DateTime, v UInt64)
ENGINE = MergeTree ORDER BY id
TTL ts + INTERVAL 1 DAY GROUP BY id SET v = sum(v),
    ts + INTERVAL 2 DAY DELETE
SETTINGS
    -- 0 keeps the OPTIMIZE below the only merge.
    max_number_of_merges_with_ttl_in_pool = 0,
    merge_with_ttl_timeout = 0,
    -- The merge empties the part and the assertions below have to see it.
    remove_empty_parts = 0;

INSERT INTO t_ttl_group_by_dropped SELECT number % 2, now() - INTERVAL 3 DAY, number + 1 FROM numbers(10);

-- The inserted part carries an expired GROUP BY bound, which is what could be inherited.
SELECT group_by_ttl_info.max[1] < now() FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_group_by_dropped' AND active;

OPTIMIZE TABLE t_ttl_group_by_dropped FINAL;

SELECT count() FROM t_ttl_group_by_dropped;

-- The emptied part holds no GROUP BY bound at all.
SELECT rows, group_by_ttl_info.max[1] = toDateTime(0) FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_group_by_dropped' AND active;

DROP TABLE t_ttl_group_by_dropped;

SELECT '-- a rows WHERE TTL filters every row before the rule sees the block';

CREATE TABLE t_ttl_group_by_where_dropped (id UInt32, ts DateTime, v UInt64)
ENGINE = MergeTree ORDER BY id
TTL ts + INTERVAL 1 DAY GROUP BY id SET v = sum(v),
    ts + INTERVAL 2 DAY DELETE WHERE v > 0
SETTINGS
    max_number_of_merges_with_ttl_in_pool = 0,
    merge_with_ttl_timeout = 0,
    remove_empty_parts = 0;

INSERT INTO t_ttl_group_by_where_dropped SELECT number % 2, now() - INTERVAL 3 DAY, number + 1 FROM numbers(10);

SELECT group_by_ttl_info.max[1] < now() FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_group_by_where_dropped' AND active;

OPTIMIZE TABLE t_ttl_group_by_where_dropped FINAL;

SELECT count() FROM t_ttl_group_by_where_dropped;

SELECT rows, group_by_ttl_info.max[1] = toDateTime(0) FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_group_by_where_dropped' AND active;

DROP TABLE t_ttl_group_by_where_dropped;
