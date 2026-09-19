-- A `Nullable(Tuple(...))` element of a multi-column `IN` key. With `transform_null_in = 0` the Set stores every
-- key type without the `Nullable` wrapper, so the left-hand column has to be brought to that type without
-- throwing on its NULL rows: a row with a NULL key element is simply not in the set.

SET enable_nullable_tuple_type = 1;

SELECT 'literal set';
SELECT x, (x, 1) IN ((tuple(1), 1), (tuple(2), 1)), (x, 1) NOT IN ((tuple(1), 1), (tuple(2), 1))
FROM (SELECT CAST(arrayJoin([tuple(1), tuple(3), NULL]), 'Nullable(Tuple(UInt8))') AS x);

SELECT 'subquery set';
SELECT x, (x, 1) IN (SELECT CAST(tuple(1), 'Nullable(Tuple(UInt8))'), 1), (x, 1) NOT IN (SELECT CAST(tuple(1), 'Nullable(Tuple(UInt8))'), 1)
FROM (SELECT CAST(arrayJoin([tuple(1), tuple(3), NULL]), 'Nullable(Tuple(UInt8))') AS x);

SELECT 'subquery set with NULL rows on the right';
SELECT x, (x, 1) IN (SELECT CAST(arrayJoin([tuple(1), NULL]), 'Nullable(Tuple(UInt8))'), 1)
FROM (SELECT CAST(arrayJoin([tuple(1), tuple(3), NULL]), 'Nullable(Tuple(UInt8))') AS x);

SELECT 'every left row is NULL';
SELECT (x, 1) IN (SELECT tuple(1), 1) FROM (SELECT materialize(NULL) AS x);
SELECT (x, 1) IN ((tuple(1), 1), (tuple(2), 1)) FROM (SELECT materialize(CAST(NULL, 'Nullable(Tuple(UInt8))')) AS x);

SELECT 'transform_null_in = 1';
SELECT x, (x, 1) IN ((tuple(1), 1), (tuple(2), 1)), (x, 1) NOT IN ((tuple(1), 1), (tuple(2), 1))
FROM (SELECT CAST(arrayJoin([tuple(1), tuple(3), NULL]), 'Nullable(Tuple(UInt8))') AS x)
SETTINGS transform_null_in = 1;

SELECT 'table';
DROP TABLE IF EXISTS t_nullable_tuple_in;
CREATE TABLE t_nullable_tuple_in (id UInt32, key Nullable(Tuple(a UInt32, s String))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_nullable_tuple_in VALUES (1, (1, 'a')), (2, NULL), (3, (3, 'c')), (4, NULL);
SELECT id FROM t_nullable_tuple_in WHERE (key, id) IN (SELECT key, id FROM t_nullable_tuple_in WHERE id < 3) ORDER BY id;
SELECT id FROM t_nullable_tuple_in WHERE (key, id) NOT IN (SELECT key, id FROM t_nullable_tuple_in WHERE id < 3) ORDER BY id;
SELECT id FROM t_nullable_tuple_in WHERE (key, id) IN (((1, 'a'), 1), ((3, 'c'), 3), (NULL, 2)) ORDER BY id;
DROP TABLE t_nullable_tuple_in;

SELECT 'a NULL element is compared as a value, whether or not the tuple is wrapped in Nullable';
SELECT (x, 1) IN ((tuple(NULL), 1)), (toNullable(x), 1) IN ((tuple(NULL), 1)) FROM (SELECT CAST(tuple(NULL), 'Tuple(Nullable(UInt8))') AS x);
SELECT (x, 1) IN ((tuple(1), 1)), (toNullable(x), 1) IN ((tuple(1), 1)) FROM (SELECT CAST(tuple(NULL), 'Tuple(Nullable(UInt8))') AS x);

SELECT 'the payload of a NULL row is not converted';
SELECT x, (x, 1) IN (SELECT tuple(toUInt8(1)), 1) FROM (SELECT CAST(arrayJoin([tuple('1'), tuple('2'), NULL]), 'Nullable(Tuple(String))') AS x);
SELECT x, (x, 1) IN (SELECT tuple(toUInt8(1)), 1) FROM (SELECT CAST(arrayJoin([tuple('1'), tuple('2'), NULL]), 'Nullable(Tuple(String))') AS x) SETTINGS transform_null_in = 1;
SELECT (x, 1) IN (SELECT tuple(toUInt8(1)), 1) FROM (SELECT materialize(CAST(NULL, 'Nullable(Tuple(String))')) AS x);
SELECT (x, 1) IN (SELECT [1], 1) FROM (SELECT materialize(NULL) AS x);
