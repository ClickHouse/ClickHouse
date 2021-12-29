
SELECT groupSortedArray(5)(number) from numbers(100);

SELECT groupSortedArray(number, number) from numbers(100);

SELECT groupSortedArray(100)(number, number) from numbers(1000);

SELECT groupSortedArray(100)(number, -number) from numbers(1000);

SELECT groupSortedArray(5)(str, number) FROM (SELECT toString(number) as str, number FROM numbers(10));

SELECT groupSortedArray(5)(text) FROM (select toString(number) as text from numbers(10));

SELECT groupSortedArray(5)(text, -number) FROM (select toString(number) as text, number from numbers(10));

SELECT groupSortedArray(10)(number, number) from numbers(100); -- { serverError 42 }

SELECT groupSortedArray(10)(number) from numbers(100); -- { serverError 42 }

SELECT groupSortedArray(5)(number, text) FROM (select toString(number) as text, number from numbers(10)); -- { serverError 43 }

SELECT groupSortedArray(5)((number,text)) from (SELECT toString(number) as text, number FROM numbers(100));

DROP TABLE IF EXISTS test;
DROP VIEW IF EXISTS mv_test;
CREATE TABLE test (`n` String, `h` Int64) ENGINE = MergeTree ORDER BY n;
CREATE MATERIALIZED VIEW mv_test (`n` String, `h` AggregateFunction(groupSortedArray(2), Int64, Int64)) ENGINE = AggregatingMergeTree ORDER BY n AS SELECT n, groupSortedArrayState(2)(h, h) as h FROM test GROUP BY n;
INSERT INTO test VALUES ('pablo',1)('pablo', 2)('luis', 1)('luis', 3)('pablo', 5)('pablo',4)('pablo', 5)('luis', 6)('luis', 7)('pablo', 8)('pablo',9)('pablo',10)('luis',11)('luis',12)('pablo',13);
SELECT n, groupSortedArrayMerge(2)(h) from mv_test GROUP BY n;

DROP TABLE IF EXISTS test;
DROP VIEW IF EXISTS mv_test;
CREATE TABLE test (`n` String, `h` Int64) ENGINE = MergeTree ORDER BY n;
CREATE MATERIALIZED VIEW mv_test (`n` String, `h` AggregateFunction(groupSortedArray(2), Int64)) ENGINE = AggregatingMergeTree ORDER BY n AS SELECT n, groupSortedArrayState(2)(h) as h FROM test GROUP BY n;
INSERT INTO test VALUES ('pablo',1)('pablo', 2)('luis', 1)('luis', 3)('pablo', 5)('pablo',4)('pablo', 5)('luis', 6)('luis', 7)('pablo', 8)('pablo',9)('pablo',10)('luis',11)('luis',12)('pablo',13);
SELECT n, groupSortedArrayMerge(2)(h) from mv_test GROUP BY n;
DROP TABLE test;
DROP VIEW mv_test;