-- Tags: no-fasttest

SET optimize_functions_to_subcolumns = 1;

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

SELECT '-- results are unchanged';
SELECT count() FROM t_file WHERE tup.1 = 555;
SELECT count() FROM t_merge WHERE tup.1 = 555;
SELECT count() FROM t_file WHERE tup.2 = '7';
SELECT count() FROM t_file WHERE nn IS NOT NULL;
SELECT count() FROM t_file WHERE length(arr) = 2;

DROP TABLE t_merge;
DROP TABLE t_file;
