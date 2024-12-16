SELECT groupArraySorted(5)(number) FROM numbers(100);

SELECT groupArraySorted(10)(number) FROM numbers(5);

SELECT groupArraySorted(100)(number) FROM numbers(1000);

SELECT groupArraySorted(30)(str) FROM (SELECT toString(number) as str FROM numbers(30));

SELECT groupArraySorted(10)(toInt64(number/2)) FROM numbers(100);

DROP TABLE IF EXISTS test;
CREATE TABLE test (a Array(UInt64)) engine=MergeTree ORDER BY a;
INSERT INTO test VALUES ([3,4,5,6]), ([1,2,3,4]), ([2,3,4,5]);
SELECT groupArraySorted(3)(a) FROM test;
DROP TABLE test;

CREATE TABLE IF NOT EXISTS test (id Int32, data Tuple(Int32, Int32)) ENGINE = MergeTree() ORDER BY id;
INSERT INTO test (id, data) VALUES (1, (100, 200)), (2, (15, 25)), (3, (2, 1)), (4, (30, 60));
SELECT groupArraySorted(4)(data) FROM test;
DROP TABLE test;

CREATE TABLE IF NOT EXISTS test (id Int32, data Decimal32(2)) ENGINE = MergeTree() ORDER BY id;
INSERT INTO test (id, data) VALUES (1, 12.5), (2, 0.2), (3, 6.6), (4, 2.2);
SELECT groupArraySorted(4)(data) FROM test;
DROP TABLE test;

CREATE TABLE IF NOT EXISTS test (id Int32, data FixedString(3)) ENGINE = MergeTree() ORDER BY id;
INSERT INTO test (id, data) VALUES (1, 'AAA'), (2, 'bbc'), (3, 'abc'), (4, 'aaa'), (5, 'Aaa');
SELECT groupArraySorted(5)(data) FROM test;
DROP TABLE test;

CREATE TABLE test (id Decimal(76, 53), str String) ENGINE = MergeTree ORDER BY id;
INSERT INTO test SELECT number, 'test' FROM numbers(1000000);
SELECT count(id) FROM test;
SELECT count(concat(toString(id), 'a')) FROM test;
DROP TABLE test;

CREATE TABLE test (id UInt64, agg AggregateFunction(groupArraySorted(2), UInt64)) engine=MergeTree ORDER BY id;
INSERT INTO test SELECT 1, groupArraySortedState(2)(number) FROM numbers(10);
SELECT groupArraySortedMerge(2)(agg) FROM test;
DROP TABLE test;
