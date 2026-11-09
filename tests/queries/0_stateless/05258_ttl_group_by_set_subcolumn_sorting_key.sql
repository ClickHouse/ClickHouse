-- `TTL GROUP BY ... SET` must keep the part sorted when the sorting key is a subcolumn of a column assigned by `SET`.
-- A merge calculates `n.a` before the TTL, so the sorting key must be recalculated from `n` after `SET`.
-- https://github.com/ClickHouse/ClickHouse/issues/121861

DROP TABLE IF EXISTS t_ttl_group_by_subcolumn_key;

CREATE TABLE t_ttl_group_by_subcolumn_key (n Tuple(a UInt32, b UInt32), ts DateTime('UTC'), v UInt32)
ENGINE = MergeTree ORDER BY n.a
TTL ts + toIntervalDay(1) GROUP BY n.a SET n = tuple(toUInt32(1000) - max(v), toUInt32(0)), ts = max(ts) + INTERVAL 100 YEAR
SETTINGS index_granularity = 1;

INSERT INTO t_ttl_group_by_subcolumn_key
SELECT tuple(toUInt32(number), toUInt32(0)), '2000-06-09 10:00:00', toUInt32(number) FROM numbers(1, 20);

OPTIMIZE TABLE t_ttl_group_by_subcolumn_key FINAL;

SELECT count() FROM t_ttl_group_by_subcolumn_key WHERE n.a = 999;
SELECT countIf(n.a = 999) FROM t_ttl_group_by_subcolumn_key;
SELECT '-- The primary index corresponds to the data';
SELECT n.a FROM t_ttl_group_by_subcolumn_key ORDER BY _part_offset;
SELECT * EXCEPT part_name FROM mergeTreeIndex(currentDatabase(), t_ttl_group_by_subcolumn_key) WHERE rows_in_granule > 0 ORDER BY mark_number;

-- A debug build checks that the part is sorted when it is merged again.
OPTIMIZE TABLE t_ttl_group_by_subcolumn_key FINAL;
SELECT count() FROM t_ttl_group_by_subcolumn_key;

DROP TABLE t_ttl_group_by_subcolumn_key;

