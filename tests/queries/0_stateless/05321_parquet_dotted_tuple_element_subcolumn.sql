-- Tags: no-fasttest
-- A tuple element with a dot in its name, like `a.b`, is read as its own subcolumn from Parquet
-- when another element, like `a`, has a name that is a prefix of it.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_dotted_1.parquet', Parquet, 'id Int32, s Tuple(a Tuple(c Int32), `a.b` Int32)')
SELECT number, tuple(tuple(number * 10), number + 100) FROM numbers(3);

SELECT id, s.`a.b` FROM file(currentDatabase() || '_dotted_1.parquet') ORDER BY id;
SELECT id FROM file(currentDatabase() || '_dotted_1.parquet') WHERE s.`a.b` = 101;
SELECT s.`a.b`, s.a, s.a.c FROM file(currentDatabase() || '_dotted_1.parquet') ORDER BY id;

INSERT INTO FUNCTION file(currentDatabase() || '_dotted_2.parquet', Parquet, 'id Int32, s Tuple(a Tuple(c Int32), `a.b` Tuple(x Int32, y Nullable(String)))')
SELECT number, tuple(tuple(number * 10), tuple(number + 100, if(number = 1, NULL, 'q'))) FROM numbers(3);

SELECT id, s.`a.b`.x, s.`a.b`.y, s.`a.b`.y.null FROM file(currentDatabase() || '_dotted_2.parquet') ORDER BY id;

INSERT INTO FUNCTION file(currentDatabase() || '_dotted_3.parquet', Parquet, 'id Int32, s Array(Tuple(a Tuple(c Int32), `a.b` Int32))')
SELECT number, [tuple(tuple(number * 10), number + 100), tuple(tuple(1), 2)] FROM numbers(3);

SELECT id, s.`a.b`, s.a.c FROM file(currentDatabase() || '_dotted_3.parquet') ORDER BY id;

-- `s.a.b` also names the path `b` of the JSON `a`; it is the element `a.b`, as when the whole `s` is read.
INSERT INTO FUNCTION file(currentDatabase() || '_dotted_4.parquet', Parquet, 'id Int32, s Tuple(a JSON, `a.b` UInt32)')
SELECT number, tuple('{"b": 7}'::JSON, number + 100) FROM numbers(3);

SELECT id, s.`a.b` FROM file(currentDatabase() || '_dotted_4.parquet', Parquet, 'id Int32, s Tuple(a JSON, `a.b` UInt32)') ORDER BY id;
SELECT count() FROM file(currentDatabase() || '_dotted_4.parquet', Parquet, 'id Int32, s Tuple(a JSON, `a.b` UInt32)') WHERE s.`a.b` = 101;
