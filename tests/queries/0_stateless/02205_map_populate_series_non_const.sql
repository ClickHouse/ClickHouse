DROP TABLE IF EXISTS 02005_test_table;
CREATE TABLE 02005_test_table
(
    value Map(Int64, Int64)
)
ENGINE = TinyLog;

SELECT 'mapPopulateSeries with map';

SELECT 'Without max key';

SELECT mapPopulateSeries(value) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

INSERT INTO 02005_test_table VALUES (map(0, 5));
SELECT mapPopulateSeries(value) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

INSERT INTO 02005_test_table VALUES (map(0, 5, 5, 10));
SELECT mapPopulateSeries(value) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

INSERT INTO 02005_test_table VALUES (map(-5, -5, 0, 5, 5, 10));
SELECT mapPopulateSeries(value) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

INSERT INTO 02005_test_table VALUES (map(-5, -5, 0, 5, 5, 10, 10, 15));
SELECT mapPopulateSeries(value) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

SELECT 'With max key';

SELECT mapPopulateSeries(value, materialize(20)) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

INSERT INTO 02005_test_table VALUES (map(0, 5));
SELECT mapPopulateSeries(value, materialize(20)) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

INSERT INTO 02005_test_table VALUES (map(0, 5, 5, 10));
SELECT mapPopulateSeries(value, materialize(20)) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

INSERT INTO 02005_test_table VALUES (map(-5, -5, 0, 5, 5, 10));
SELECT mapPopulateSeries(value) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

INSERT INTO 02005_test_table VALUES (map(-5, -5, 0, 5, 5, 10, 10, 15));
SELECT mapPopulateSeries(value, materialize(20)) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

SELECT 'Possible verflow';

SELECT mapPopulateSeries(map(toUInt64(18446744073709551610), toUInt64(5)), 18446744073709551615);
SELECT mapPopulateSeries(map(toUInt64(18446744073709551615), toUInt64(5)), 18446744073709551615);

SELECT 'Duplicate keys';

SELECT mapPopulateSeries(map(1, 4, 1, 5, 5, 6));
SELECT mapPopulateSeries(map(1, 4, 1, 5, 5, 6), materialize(10));

DROP TABLE 02005_test_table;

DROP TABLE IF EXISTS 02005_test_table;
CREATE TABLE 02005_test_table
(
    key Array(Int64),
    value Array(Int64)
)
ENGINE = TinyLog;

SELECT 'mapPopulateSeries with two arrays';
SELECT 'Without max key';

SELECT mapPopulateSeries(key, value) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

INSERT INTO 02005_test_table VALUES ([0], [5]);
SELECT mapPopulateSeries(key, value) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

INSERT INTO 02005_test_table VALUES ([0, 5], [5, 10]);
SELECT mapPopulateSeries(key, value) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

INSERT INTO 02005_test_table VALUES ([-5, 0, 5], [-5, 5, 10]);
SELECT mapPopulateSeries(key, value) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

INSERT INTO 02005_test_table VALUES ([-5, 0, 5, 10], [-5, 5, 10, 15]);
SELECT mapPopulateSeries(key, value) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

SELECT 'With max key';

SELECT mapPopulateSeries(key, value, materialize(20)) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

INSERT INTO 02005_test_table VALUES ([0], [5]);
SELECT mapPopulateSeries(key, value, materialize(20)) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

INSERT INTO 02005_test_table VALUES ([0, 5], [5, 10]);
SELECT mapPopulateSeries(key, value, materialize(20)) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

INSERT INTO 02005_test_table VALUES ([-5, 0, 5], [-5, 5, 10]);
SELECT mapPopulateSeries(key, value, materialize(20)) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

INSERT INTO 02005_test_table VALUES ([-5, 0, 5, 10], [-5, 5, 10, 15]);
SELECT mapPopulateSeries(key, value, materialize(20)) FROM 02005_test_table;
TRUNCATE TABLE 02005_test_table;

SELECT 'Possible verflow';

SELECT mapPopulateSeries([18446744073709551610], [5], 18446744073709551615);
SELECT mapPopulateSeries([18446744073709551615], [5], 18446744073709551615);

SELECT 'Duplicate keys';

SELECT mapPopulateSeries([1, 1, 5], [4, 5, 6]);
SELECT mapPopulateSeries([1, 1, 5], [4, 5, 6], materialize(10));

DROP TABLE 02005_test_table;
