-- Tags: no-random-settings, no-random-merge-tree-settings
SET allow_experimental_column_ids = 1;

SELECT 'Test 1: basic wide part with ADD COLUMN';

DROP TABLE IF EXISTS t_column_ids_basic;

CREATE TABLE t_column_ids_basic
(
    a UInt64,
    b String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_column_ids';

INSERT INTO t_column_ids_basic VALUES (1, 'one');
ALTER TABLE t_column_ids_basic ADD COLUMN c Nullable(String);
INSERT INTO t_column_ids_basic (a, b, c) VALUES (2, 'two', 'second');

SELECT * FROM t_column_ids_basic ORDER BY a;
SELECT column, column_id
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_column_ids_basic' AND active AND NOT startsWith(column, '_')
ORDER BY column;

DROP TABLE t_column_ids_basic;

SELECT 'Test 2: virtual columns';

DROP TABLE IF EXISTS t_column_ids_virtuals;

CREATE TABLE t_column_ids_virtuals
(
    a UInt64
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_column_ids',
    enable_block_number_column = 1,
    enable_block_offset_column = 1;

INSERT INTO t_column_ids_virtuals VALUES (1);
ALTER TABLE t_column_ids_virtuals ADD COLUMN c UInt64 DEFAULT a + 10;
INSERT INTO t_column_ids_virtuals (a, c) VALUES (2, 22);

SELECT a, c FROM t_column_ids_virtuals ORDER BY a;
SELECT column, column_id
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_column_ids_virtuals' AND active AND NOT startsWith(column, '_')
ORDER BY column;
SELECT countDistinct(_block_number), sum(_block_offset) FROM t_column_ids_virtuals;

DELETE FROM t_column_ids_virtuals WHERE a = 1;
SELECT count() FROM t_column_ids_virtuals;
SELECT count()
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_column_ids_virtuals' AND active AND column = '_row_exists';

INSERT INTO t_column_ids_virtuals (a, c) VALUES (3, 33);
SELECT a, c FROM t_column_ids_virtuals ORDER BY a;

DROP TABLE t_column_ids_virtuals;

SELECT 'Test 3: complex types';

SET flatten_nested = 0;
DROP TABLE IF EXISTS t_column_ids_complex;

CREATE TABLE t_column_ids_complex
(
    n Nested(x UInt64, y String),
    t Tuple(x UInt64, y String),
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_column_ids';

INSERT INTO t_column_ids_complex VALUES ([(1, 'a'), (2, 'b')], (3, 'c'), map('k', 4));
SELECT n.x, n.y, t.x, t.y, mapKeys(m), mapValues(m) FROM t_column_ids_complex;
SELECT column, column_id
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_column_ids_complex' AND active AND NOT startsWith(column, '_')
ORDER BY column;

DROP TABLE t_column_ids_complex;
SET flatten_nested = 1;
