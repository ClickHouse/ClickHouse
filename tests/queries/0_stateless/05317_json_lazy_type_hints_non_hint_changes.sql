-- Tags: no-fasttest
-- Test that with lazy JSON type hints enabled, changing SKIP, SKIP REGEXP, max_dynamic_paths or max_dynamic_types still rewrites existing parts

DROP TABLE IF EXISTS t_json_lazy_non_hint;
SET enable_json_lazy_type_hints = 1;
SET mutations_sync = 1;

DROP TABLE IF EXISTS t_json_lazy_non_hint;
CREATE TABLE t_json_lazy_non_hint (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_lazy_non_hint VALUES (1, '{"a": 1, "b": "x", "c": "y"}');

ALTER TABLE t_json_lazy_non_hint MODIFY COLUMN j JSON(a UInt32, SKIP b);
SELECT 'SKIP', type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_json_lazy_non_hint' AND column = 'j' AND active ORDER BY name;
SELECT id, j, j.b FROM t_json_lazy_non_hint ORDER BY id;

ALTER TABLE t_json_lazy_non_hint MODIFY COLUMN j JSON(a UInt32, SKIP b, SKIP REGEXP '^c');
SELECT 'SKIP REGEXP', type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_json_lazy_non_hint' AND column = 'j' AND active ORDER BY name;
SELECT id, j, j.c FROM t_json_lazy_non_hint ORDER BY id;

ALTER TABLE t_json_lazy_non_hint MODIFY COLUMN j JSON(max_dynamic_paths = 8, a UInt32, SKIP b, SKIP REGEXP '^c');
SELECT 'max_dynamic_paths', type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_json_lazy_non_hint' AND column = 'j' AND active ORDER BY name;

ALTER TABLE t_json_lazy_non_hint MODIFY COLUMN j JSON(max_dynamic_paths = 8, max_dynamic_types = 4, a UInt32, SKIP b, SKIP REGEXP '^c');
SELECT 'max_dynamic_types', type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_json_lazy_non_hint' AND column = 'j' AND active ORDER BY name;

-- A change of the typed paths alone stays metadata-only: the part keeps `a UInt32`.
ALTER TABLE t_json_lazy_non_hint MODIFY COLUMN j JSON(max_dynamic_paths = 8, max_dynamic_types = 4, a UInt64, SKIP b, SKIP REGEXP '^c');
SELECT 'typed paths only', type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_json_lazy_non_hint' AND column = 'j' AND active ORDER BY name;
SELECT id, j, toTypeName(j) FROM t_json_lazy_non_hint ORDER BY id;

DROP TABLE t_json_lazy_non_hint;
