SELECT groupArraySorted(5)(number) from numbers(100);

SELECT groupArraySorted(number) from numbers(100);

SELECT groupArraySorted(100)(number) from numbers(1000);

SELECT groupArraySorted(5)(str) FROM (SELECT toString(number) as str FROM numbers(10));

SELECT groupArraySorted(5)(text) FROM (select toString(number) as text from numbers(10));

SELECT groupArraySorted(10)(toInt64(number/2)) FROM numbers(100);

DROP TABLE IF EXISTS test;
CREATE TABLE test (a Array(UInt64)) engine=MergeTree ORDER BY a;
INSERT INTO test VALUES ([3,4,5,6]), ([1,2,3,4]), ([2,3,4,5]);
SELECT groupArraySorted(a) FROM test;
DROP TABLE test;

DROP TABLE IF EXISTS test;
CREATE TABLE IF NOT EXISTS test (id Int32, data Tuple(Int32, Int32)) ENGINE = MergeTree() ORDER BY id;
INSERT INTO test (id, data) VALUES (1, (100, 200)), (2, (15, 25)), (3, (2, 1)), (4, (30, 60));
SELECT groupArraySorted(data) FROM test;
