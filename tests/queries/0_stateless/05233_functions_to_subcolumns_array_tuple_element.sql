SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_array_tuple_element;

CREATE TABLE t_array_tuple_element
(
    id UInt64,
    a Array(Tuple(
        code UInt32,
        nullable Nullable(Int32),
        category LowCardinality(String),
        payload String)),
    b Array(Nullable(Tuple(code UInt32, payload String)))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_array_tuple_element VALUES
    (1, [(10, 100, 'alpha', 'payload-10'), (11, NULL, 'beta', 'payload-11')],
        [CAST((100, 'b-100') AS Nullable(Tuple(code UInt32, payload String))), NULL]),
    (2, [], []),
    (3, [(30, -3, 'gamma', 'payload-30'), (31, 4, 'gamma', 'payload-31'), (32, NULL, 'delta', 'payload-32')],
        [CAST((300, 'b-300') AS Nullable(Tuple(code UInt32, payload String)))]);

SELECT '-- named Array(Tuple) fields preserve values and types';
SELECT
    id,
    tupleElement(a, 'code'),
    a.code,
    tupleElement(a, 'nullable'),
    tupleElement(a, 'category'),
    toTypeName(tupleElement(a, 'code')),
    toTypeName(tupleElement(a, 'nullable')),
    toTypeName(tupleElement(a, 'category'))
FROM t_array_tuple_element
ORDER BY id;

SELECT '-- positional Array(Tuple) fields preserve values and types';
SELECT
    id,
    tupleElement(a, 1),
    tupleElement(a, 2),
    tupleElement(a, -1),
    toTypeName(tupleElement(a, -1))
FROM t_array_tuple_element
ORDER BY id;

SELECT '-- Array(Nullable(Tuple)) remains on the function path';
SELECT id, tupleElement(b, 'code'), toTypeName(tupleElement(b, 'code'))
FROM t_array_tuple_element
ORDER BY id;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT tupleElement(b, 'code') FROM t_array_tuple_element)
WHERE explain ILIKE '%column_name: b.code%';

SELECT '-- tupleElement reaches the selected Array subcolumn';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT tupleElement(a, 'code') FROM t_array_tuple_element)
WHERE explain ILIKE '%column_name: a.code%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT a.code FROM t_array_tuple_element)
WHERE explain ILIKE '%column_name: a.code%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT tupleElement(a, 'code') FROM t_array_tuple_element)
WHERE explain ILIKE '%function_name: tupleElement%';

SELECT '-- a full-column use does not add a duplicate subcolumn read';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT a, tupleElement(a, 'code') FROM t_array_tuple_element)
WHERE explain ILIKE '%column_name: a.code%';

DROP TABLE t_array_tuple_element;

CREATE TABLE t_array_tuple_collision
(
    a Array(Tuple(code UInt32, payload String)),
    `a.code` Array(UInt64)
)
ENGINE = Memory;

CREATE TABLE t_array_tuple_case_collision
(
    a Array(Tuple(code UInt32, payload String)),
    `A.CODE` Array(UInt64)
)
ENGINE = MergeTree ORDER BY tuple();

CREATE TABLE t_array_tuple_dotted
(
    a Array(Tuple(nested Tuple(code UInt32), `nested.code` UInt32, payload String))
)
ENGINE = Memory;

SELECT '-- physical dotted columns and case collisions are not rewritten';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT tupleElement(a, 'code') FROM t_array_tuple_collision)
WHERE explain ILIKE '%column_name: a.code%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT tupleElement(a, 'code') FROM t_array_tuple_case_collision)
WHERE explain ILIKE '%column_name: a.code%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT tupleElement(a, 'nested.code') FROM t_array_tuple_dotted)
WHERE explain ILIKE '%column_name: a.nested.code%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT tupleElement(a, 'nested') FROM t_array_tuple_dotted)
WHERE explain ILIKE '%column_name: a.nested%';

DROP TABLE t_array_tuple_collision;
DROP TABLE t_array_tuple_case_collision;
DROP TABLE t_array_tuple_dotted;
