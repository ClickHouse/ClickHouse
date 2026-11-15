-- Tags: no-fasttest
-- Test mutations on a part written before a lazy JSON type hint was added

DROP TABLE IF EXISTS t_json_lazy_mutations;
SET enable_json_lazy_type_hints = 1;
SET mutations_sync = 1;

DROP TABLE IF EXISTS t_json_lazy_mutations;
CREATE TABLE t_json_lazy_mutations (id UInt32, v UInt32, j JSON) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
         enable_block_number_column = 0, enable_block_offset_column = 0, auto_statistics_types = '';

INSERT INTO t_json_lazy_mutations VALUES (1, 10, '{"a": 1, "b": "x"}'), (2, 20, '{"a": 2}'), (3, 30, '{"a": "3"}');
ALTER TABLE t_json_lazy_mutations MODIFY COLUMN j JSON(a UInt32);

-- A mutation of another column hardlinks `j`, so the part keeps the pre-hint type.
ALTER TABLE t_json_lazy_mutations UPDATE v = v + 1 WHERE 1;
SELECT 'update of another column', type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_json_lazy_mutations' AND column = 'j' AND active ORDER BY name;
SELECT id, v, j FROM t_json_lazy_mutations ORDER BY id;

DETACH TABLE t_json_lazy_mutations;
ATTACH TABLE t_json_lazy_mutations;
SELECT id, v, j FROM t_json_lazy_mutations ORDER BY id;

-- A mutation of `j` itself rewrites the part with the hinted type.
ALTER TABLE t_json_lazy_mutations UPDATE j = '{"a": 42, "c": true}' WHERE id = 2;
SELECT 'update of the JSON column', type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_json_lazy_mutations' AND column = 'j' AND active ORDER BY name;
SELECT id, v, j FROM t_json_lazy_mutations ORDER BY id;

DROP TABLE t_json_lazy_mutations;
