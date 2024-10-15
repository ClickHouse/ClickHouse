-- { echoOn }
-- https://github.com/ClickHouse/ClickHouse/issues/68895
SELECT arrayMax(x -> toFixedString('.', 1), []);

-- https://github.com/ClickHouse/ClickHouse/issues/69600
SELECT arrayMax(x -> (-x), [1, 2, 4]) AS res;
SELECT arrayMax(x -> toUInt16(-x), [1, 2, 4]) AS res;

-- https://github.com/ClickHouse/ClickHouse/pull/69640
SELECT arrayMin(x1 -> (x1 * toNullable(-1)), materialize([1, 2, 3]));
SELECT arrayMin(x1 -> x1 * -1, [1,2,3]);

DROP TABLE IF EXISTS test_aggregation_array;
CREATE TABLE test_aggregation_array (x Array(Int)) ENGINE=MergeTree() ORDER by tuple();
INSERT INTO test_aggregation_array VALUES ([1,2,3,4,5,6]), ([]), ([1,2,3]);

SELECT [arrayMin(x1 -> (x1 * materialize(-1)), [toNullable(toUInt256(0)), materialize(4)])], arrayMin([arrayMin([0])]) FROM test_aggregation_array GROUP BY arrayAvg([1]), [0, toUInt256(8)] WITH CUBE SETTINGS allow_experimental_analyzer = 1;
SELECT [arrayMin([3, arrayMin([toUInt128(8)]), 4, 5]), arrayMax([materialize(1)]), arrayMin([arrayMax([1]), 2]), 2], arrayMin([0, toLowCardinality(8)]),     2, arrayMax(x1 -> (x1 * -1), x) FROM test_aggregation_array;

select arrayMax(x -> x.1, [(1, 'a'), (0, 'b')]);
select arrayMin(x -> x.2, [(1, 'a'), (0, 'b')]);

-- Extra validation of generic arrayMin/arrayMax
WITH [(1,2),(1,3)] AS t SELECT arrayMin(t), arrayMax(t);
WITH [map('a', 1, 'b', 2), map('a',1,'b',3)] AS t SELECT arrayMin(t), arrayMax(t);
WITH [map('a', 1, 'b', 2, 'c', 10), map('a',1,'b',3, 'c', 0)] AS t SELECT arrayMin(x -> x['c'], t), arrayMax(x -> x['c'], t);
