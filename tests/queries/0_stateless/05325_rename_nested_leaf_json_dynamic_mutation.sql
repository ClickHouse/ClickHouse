-- Renaming a `JSON` or `Dynamic` leaf of a `Nested` column rewrites the part. The mutation must
-- read the leaf under its old name instead of filling the new part with defaults.
-- https://github.com/ClickHouse/ClickHouse/issues/118157

DROP TABLE IF EXISTS t_rename_nested_json;
DROP TABLE IF EXISTS t_rename_nested_dynamic;

CREATE TABLE t_rename_nested_json (id UInt8, `n.a` Array(UInt8), `n.b` Array(JSON))
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_rename_nested_json VALUES (1, [7, 8], ['{"x":1}', '{"x":2}']);
ALTER TABLE t_rename_nested_json RENAME COLUMN n.b TO n.z SETTINGS mutations_sync = 2;
SELECT id, n.a, n.z FROM t_rename_nested_json;
ALTER TABLE t_rename_nested_json RENAME COLUMN n.z TO n.b SETTINGS mutations_sync = 2;
SELECT id, n.a, n.b FROM t_rename_nested_json;

CREATE TABLE t_rename_nested_dynamic (id UInt8, `n.a` Array(UInt8), `n.b` Array(Dynamic))
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_rename_nested_dynamic VALUES (1, [7, 8], [1, 'x']);
ALTER TABLE t_rename_nested_dynamic RENAME COLUMN n.b TO n.z SETTINGS mutations_sync = 2;
SELECT id, n.a, n.z FROM t_rename_nested_dynamic;

DROP TABLE t_rename_nested_json;
DROP TABLE t_rename_nested_dynamic;
