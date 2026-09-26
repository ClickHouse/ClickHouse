-- Tags: no-fasttest

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
-- The pass disables itself for the whole query when a join can wrap results in Nullable, so a
-- comma-join arm below would assert 0 instead. Stress workers set join_use_nulls = 1.
SET join_use_nulls = 0;

DROP TABLE IF EXISTS t_file;
DROP TABLE IF EXISTS t_merge;

CREATE TABLE t_file (id UInt64, tup Tuple(`1` UInt64, `2` String), nn Nullable(UInt64), arr Array(UInt64))
ENGINE = File(Parquet);
INSERT INTO t_file SELECT number, tuple(number, toString(number % 31)), number, range(number % 4) FROM numbers(1000);

CREATE TABLE t_merge AS t_file ENGINE = Merge(currentDatabase(), '^t_file$');

SELECT '-- tuple element becomes a subcolumn read';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT id FROM t_file WHERE tup.1 = 555)
WHERE explain ILIKE '%column_name: tup.1%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT id FROM t_merge WHERE tup.1 = 555)
WHERE explain ILIKE '%column_name: tup.1%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT id FROM file('nonexistent_04708.parquet', Parquet, 'id UInt64, tup Tuple(`1` UInt64, `2` String)') WHERE tup.1 = 555)
WHERE explain ILIKE '%column_name: tup.1%';

SELECT '-- other subcolumn kinds stay whole-column functions';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT id FROM t_file WHERE nn IS NOT NULL)
WHERE explain ILIKE '%column_name: nn.null%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT id FROM t_file WHERE length(arr) = 2)
WHERE explain ILIKE '%column_name: arr.size0%';

SELECT '-- two distinct table function sources keep separate identities';
SELECT count() FROM (
    EXPLAIN QUERY TREE SELECT a.id FROM
        file('nonexistent_04708_a.parquet', Parquet, 'id UInt64, tup Tuple(`1` UInt64, `2` String)') AS a,
        file('nonexistent_04708_b.parquet', Parquet, 'id UInt64, tup Tuple(`1` UInt64, `2` String)') AS b
    WHERE a.tup.1 = 1 AND b.tup.1 = 2)
WHERE explain ILIKE '%column_name: tup.1%';

-- Only `a` reads the element, `b` reads the whole tuple: a key shared by both sources would
-- weigh 2 uses against 1 rewrite and refuse to optimize either.
SELECT '-- a shared StorageID must not pool counts across sources';
SELECT count() FROM (
    EXPLAIN QUERY TREE SELECT a.tup.1, b.tup FROM
        file('nonexistent_04708_a.parquet', Parquet, 'id UInt64, tup Tuple(`1` UInt64, `2` String)') AS a,
        file('nonexistent_04708_b.parquet', Parquet, 'id UInt64, tup Tuple(`1` UInt64, `2` String)') AS b)
WHERE explain ILIKE '%column_name: tup.1%';

SELECT '-- a repeated real table is keyed per occurrence';
SELECT count() FROM (
    EXPLAIN QUERY TREE SELECT x.tup.1, y.tup FROM t_file AS x, t_file AS y)
WHERE explain ILIKE '%column_name: tup.1%';

SELECT '-- results are unchanged';
SELECT count() FROM t_file WHERE tup.1 = 555;
SELECT count() FROM t_merge WHERE tup.1 = 555;
SELECT count() FROM t_file WHERE tup.2 = '7';
SELECT count() FROM t_file WHERE nn IS NOT NULL;
SELECT count() FROM t_file WHERE length(arr) = 2;

DROP TABLE t_merge;
DROP TABLE t_file;

-- A tuple that holds both `a.b` and an `a` with a `b` element flattens two different elements to
-- the same name `t.a.b`. An exact element-name lookup binds the dotted element, a prefix walk over
-- a file schema binds the nested one, so the rewrite must be refused for the dotted element.
SELECT '-- a dotted element name colliding with a nested path is not rewritten';
SELECT count() FROM (
    EXPLAIN QUERY TREE SELECT 1 FROM file('nonexistent_04708.parquet', Parquet, 't Tuple(a Tuple(b UInt64), `a.b` UInt64)')
    WHERE tupleElement(t, 'a.b') = 5)
WHERE explain ILIKE '%column_name: t.a.b%';
SELECT count() FROM (
    EXPLAIN QUERY TREE SELECT 1 FROM file('nonexistent_04708.parquet', Parquet, 't Tuple(`a.b` UInt64, a Tuple(b UInt64))')
    WHERE tupleElement(t, 'a.b') = 5)
WHERE explain ILIKE '%column_name: t.a.b%';

-- A reader may match field names case-insensitively, so a collision that differs only in case
-- is refused too.
SELECT count() FROM (
    EXPLAIN QUERY TREE SELECT 1 FROM file('nonexistent_04708.parquet', Parquet, 't Tuple(A Tuple(B UInt64), `a.b` UInt64)')
    WHERE tupleElement(t, 'a.b') = 5)
WHERE explain ILIKE '%column_name: t.a.b%';
SELECT count() FROM (
    EXPLAIN QUERY TREE SELECT 1 FROM file('nonexistent_04708.parquet', Parquet, 't Tuple(a Tuple(b UInt64), `A.B` UInt64)')
    WHERE tupleElement(t, 'A.B') = 5)
WHERE explain ILIKE '%column_name: t.A.B%';

SELECT '-- a dotted element name with no colliding nested path is still rewritten';
SELECT count() FROM (
    EXPLAIN QUERY TREE SELECT 1 FROM file('nonexistent_04708.parquet', Parquet, 't Tuple(`a.b` UInt64, c UInt64)')
    WHERE tupleElement(t, 'a.b') = 5)
WHERE explain ILIKE '%column_name: t.a.b%';
SELECT count() FROM (
    EXPLAIN QUERY TREE SELECT 1 FROM file('nonexistent_04708.parquet', Parquet, 't Tuple(a Tuple(b UInt64), c UInt64)')
    WHERE tupleElement(tupleElement(t, 'a'), 'b') = 5)
WHERE explain ILIKE '%column_name: t.a%';
