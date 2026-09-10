SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_byte_size_string_subcolumn;

CREATE TABLE t_byte_size_string_subcolumn
(
    id UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS string_serialization_version = 'with_size_stream';

INSERT INTO t_byte_size_string_subcolumn VALUES
    (1, ''),
    (2, 'a'),
    (3, 'hello'),
    (4, '0123456789');

EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1
SELECT byteSize(s)
FROM t_byte_size_string_subcolumn;

SELECT id, byteSize(s)
FROM t_byte_size_string_subcolumn
ORDER BY id;

SELECT id, byteSize(s)
FROM t_byte_size_string_subcolumn
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT sum(byteSize(s)) AS actual, sum(length(s)) + count() * 8 AS equivalent
FROM t_byte_size_string_subcolumn;

SELECT id, byteSize(s, 1)
FROM t_byte_size_string_subcolumn
ORDER BY id;

EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1
SELECT byteSize(s), byteSize(s, 1)
FROM t_byte_size_string_subcolumn;

DROP TABLE t_byte_size_string_subcolumn;

DROP TABLE IF EXISTS t_byte_size_sparse;

CREATE TABLE t_byte_size_sparse
(
    id UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.1,
    string_serialization_version = 'with_size_stream';

INSERT INTO t_byte_size_sparse
SELECT
    number,
    if(number % 5 = 0, toString(number), '')
FROM numbers(200);

SELECT column, serialization_kind
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_byte_size_sparse'
  AND active
ORDER BY column;

SELECT 'optimized', sum(byteSize(s))
FROM t_byte_size_sparse;

SELECT 'unoptimized', sum(byteSize(s))
FROM t_byte_size_sparse
SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE t_byte_size_sparse;
