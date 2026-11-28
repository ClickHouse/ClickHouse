-- Wide parts of flattened `Nested` arrays with a `JSON` member keep sibling values, offsets and JSON paths on read and merge.
-- https://github.com/ClickHouse/ClickHouse/issues/122499

DROP TABLE IF EXISTS t_nested_json_wide;

CREATE TABLE t_nested_json_wide (id UInt64, `n.a` Array(UInt64), `n.b` Array(JSON(t UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_nested_json_wide VALUES (1, [1, 2], ['{"t":10,"x":1}', '{"t":20,"x":2}']);
INSERT INTO t_nested_json_wide VALUES (2, [3], ['{"t":30,"x":3}']);

SELECT id, n.a, n.b.t FROM t_nested_json_wide ORDER BY id;

OPTIMIZE TABLE t_nested_json_wide FINAL;
SELECT id, n.a, n.b.t FROM t_nested_json_wide ORDER BY id;

SELECT id, n.a.size0, n.b.x FROM t_nested_json_wide ORDER BY id;

DROP TABLE t_nested_json_wide;
