-- A merge keeps the values of a column in a part without TTL info for that column,
-- even when another merged part has every value of the column expired.

DROP TABLE IF EXISTS t_column_ttl_src;
DROP TABLE IF EXISTS t_column_ttl_attach;
DROP TABLE IF EXISTS t_column_ttl_modify;
DROP TABLE IF EXISTS t_column_ttl_stopped;

CREATE TABLE t_column_ttl_src (d DateTime, x UInt64, s String DEFAULT 'dflt')
ENGINE = MergeTree ORDER BY x PARTITION BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_column_ttl_src SELECT now(), number, 'keep' FROM numbers(100);

-- A part attached from a table where the column has no TTL.
CREATE TABLE t_column_ttl_attach (d DateTime, x UInt64, s String DEFAULT 'dflt' TTL d + INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY x PARTITION BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, max_number_of_merges_with_ttl_in_pool = 0;

SYSTEM STOP MERGES t_column_ttl_attach;
INSERT INTO t_column_ttl_attach SELECT now() - INTERVAL 10 DAY, number + 1000, 'old' FROM numbers(100);
ALTER TABLE t_column_ttl_attach ATTACH PARTITION tuple() FROM t_column_ttl_src;
SYSTEM START MERGES t_column_ttl_attach;
OPTIMIZE TABLE t_column_ttl_attach FINAL;
SELECT 'attach', s, count() FROM t_column_ttl_attach GROUP BY s ORDER BY s;

-- A part written before the column got its TTL, without materializing the TTL.
CREATE TABLE t_column_ttl_modify (d DateTime, x UInt64, s String DEFAULT 'dflt')
ENGINE = MergeTree ORDER BY x PARTITION BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, max_number_of_merges_with_ttl_in_pool = 0;

INSERT INTO t_column_ttl_modify SELECT now(), number, 'keep' FROM numbers(100);
ALTER TABLE t_column_ttl_modify MODIFY COLUMN s String DEFAULT 'dflt' TTL d + INTERVAL 1 DAY SETTINGS materialize_ttl_after_modify = 0;
SYSTEM STOP MERGES t_column_ttl_modify;
INSERT INTO t_column_ttl_modify SELECT now() - INTERVAL 10 DAY, number + 1000, 'old' FROM numbers(100);
SYSTEM START MERGES t_column_ttl_modify;
OPTIMIZE TABLE t_column_ttl_modify FINAL;
SELECT 'modify', s, count() FROM t_column_ttl_modify GROUP BY s ORDER BY s;

-- The two parts are merged while TTL merges are stopped, then the merged part gets a TTL merge alone.
CREATE TABLE t_column_ttl_stopped (d DateTime, x UInt64, s String DEFAULT 'dflt' TTL d + INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY x PARTITION BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, max_number_of_merges_with_ttl_in_pool = 0;

SYSTEM STOP MERGES t_column_ttl_stopped;
INSERT INTO t_column_ttl_stopped SELECT now() - INTERVAL 10 DAY, number + 1000, 'old' FROM numbers(100);
ALTER TABLE t_column_ttl_stopped ATTACH PARTITION tuple() FROM t_column_ttl_src;
SYSTEM STOP TTL MERGES t_column_ttl_stopped;
SYSTEM START MERGES t_column_ttl_stopped;
OPTIMIZE TABLE t_column_ttl_stopped FINAL;
SELECT 'stopped, parts before TTL merge', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_column_ttl_stopped' AND active;
SELECT 'stopped, before TTL merge', s, count() FROM t_column_ttl_stopped GROUP BY s ORDER BY s;
SYSTEM START TTL MERGES t_column_ttl_stopped;
OPTIMIZE TABLE t_column_ttl_stopped FINAL;
SELECT 'stopped', s, count() FROM t_column_ttl_stopped GROUP BY s ORDER BY s;

SELECT table, part_type, count() FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('t_column_ttl_attach', 't_column_ttl_modify', 't_column_ttl_stopped')
GROUP BY ALL ORDER BY table;

DROP TABLE t_column_ttl_src;
DROP TABLE t_column_ttl_attach;
DROP TABLE t_column_ttl_modify;
DROP TABLE t_column_ttl_stopped;
