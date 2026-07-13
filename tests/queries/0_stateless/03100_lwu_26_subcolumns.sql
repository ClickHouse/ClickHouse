DROP TABLE IF EXISTS t_lwu_subcolumns;

SET enable_json_type = 1;
SET enable_lightweight_update = 1;

CREATE TABLE t_lwu_subcolumns(data JSON, arr Array(UInt32), n Nullable(String))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_subcolumns VALUES ('{"a": 1, "b": "foo"}', [1, 2, 3, 4], NULL);

UPDATE t_lwu_subcolumns SET data = '{"a": "qqww", "c": 2}' WHERE 1;
UPDATE t_lwu_subcolumns SET arr = [100, 200], n = 'aaa' WHERE 1;

SET apply_patch_parts = 0;

SELECT * FROM t_lwu_subcolumns;
SELECT data.a, data.b, data.c FROM t_lwu_subcolumns;
SELECT arr.size0 FROM t_lwu_subcolumns;
SELECT n.null FROM t_lwu_subcolumns;

SET apply_patch_parts = 1;

SELECT * FROM t_lwu_subcolumns;
SELECT data.a, data.b, data.c FROM t_lwu_subcolumns;
SELECT arr.size0 FROM t_lwu_subcolumns;
SELECT n.null FROM t_lwu_subcolumns;

SET optimize_throw_if_noop = 1;
OPTIMIZE TABLE t_lwu_subcolumns FINAL;

SET apply_patch_parts = 0;

SELECT * FROM t_lwu_subcolumns;
SELECT data.a, data.b, data.c FROM t_lwu_subcolumns;
SELECT arr.size0 FROM t_lwu_subcolumns;
SELECT n.null FROM t_lwu_subcolumns;

DROP TABLE t_lwu_subcolumns;
