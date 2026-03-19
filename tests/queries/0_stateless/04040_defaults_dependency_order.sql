SELECT 'test_diamond';
DROP TABLE IF EXISTS test_diamond;
CREATE TABLE test_diamond (
    a UInt32 DEFAULT 1,
    b UInt32 DEFAULT a * 10,
    c UInt32 DEFAULT a * 100,
    d UInt32 DEFAULT b + c
) ENGINE = Memory;

INSERT INTO test_diamond FORMAT JSONEachRow
{"a": 5}
{}
{"b": 7}

SELECT * FROM test_diamond ORDER BY a, b;
DROP TABLE test_diamond;

SELECT 'test_array';
DROP TABLE IF EXISTS test_array;
CREATE TABLE test_array (
    n UInt32 DEFAULT 3,
    arr Array(UInt32) DEFAULT range(n),
    total UInt64 DEFAULT if(length(arr) > 0, arr[1], 0)
) ENGINE = Memory;

INSERT INTO test_array FORMAT JSONEachRow
{"n": 5}
{}
{"arr": [10, 20]}

SELECT * FROM test_array ORDER BY n, arr;
DROP TABLE test_array;

SELECT 'test_wide';
DROP TABLE IF EXISTS test_wide;
CREATE TABLE test_wide (
    c1 UInt32 DEFAULT 1,
    c2 UInt32 DEFAULT c1 + 1,
    c3 UInt32 DEFAULT c2 + 1, c4 UInt32 DEFAULT c3 + 1,
    c5 UInt32 DEFAULT c4 + 1, c6 UInt32 DEFAULT c5 + 1,
    c7 UInt32 DEFAULT c6 + 1, c8 UInt32 DEFAULT c7 + 1,
    c9 UInt32 DEFAULT c8 + 1, c10 UInt32 DEFAULT c9 + 1
) ENGINE = Memory;

INSERT INTO test_wide FORMAT JSONEachRow
{"c1": 100, "c5": 500}

INSERT INTO test_wide FORMAT JSONEachRow
{}

SELECT * FROM test_wide ORDER BY ALL;
DROP TABLE test_wide;


SELECT 'test_tuple_elem';
DROP TABLE IF EXISTS test_tuple_elem;
CREATE TABLE test_tuple_elem (
    a Tuple(x Int32, y Int32) DEFAULT (1, 2),
    b Tuple(x Int32, y Int32) DEFAULT (tupleElement(a, 1) * 10, tupleElement(a, 2) * 10)
) ENGINE = Memory;

INSERT INTO test_tuple_elem FORMAT JSONEachRow
{"a": {"x": 5, "y": 6}}
{}

SELECT * FROM test_tuple_elem ORDER BY a;
DROP TABLE test_tuple_elem;

SELECT 'test_tuple_elem2: Bug, expected to have same result as test_tuple_elem, but missed dependency of b on a subcolumn:';
DROP TABLE IF EXISTS test_tuple_elem2;
CREATE TABLE test_tuple_elem2 (
    a Tuple(x Int32, y Int32) DEFAULT (1, 2),
    b Tuple(x Int32, y Int32) DEFAULT (a.x * 10, a.y * 10)
) ENGINE = Memory;

INSERT INTO test_tuple_elem2 FORMAT JSONEachRow
{"a": {"x": 5, "y": 6}}
{}

SELECT * FROM test_tuple_elem2 ORDER BY a;
DROP TABLE test_tuple_elem2;

SELECT 'test_circ_regular';
DROP TABLE IF EXISTS test_circ_regular;
CREATE TABLE test_circ_regular (
    a Int32 DEFAULT b + 1,
    b Int32 DEFAULT a + 1
) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }

SELECT 'test_circ_tuple';
DROP TABLE IF EXISTS test_circ_tuple;
CREATE TABLE test_circ_tuple (
    a Tuple(x Int32, y Int32) DEFAULT (b.x, b.x),
    b Tuple(x Int32, y Int32) DEFAULT (c.x, 2),
    c Tuple(x Int32, y Int32) DEFAULT (a.x, 2)
) ENGINE = Memory; -- { serverError UNKNOWN_IDENTIFIER }
