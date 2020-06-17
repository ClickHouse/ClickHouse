SELECT sum(DISTINCT number) FROM numbers_mt(100000);
SELECT sum(DISTINCT number % 13) FROM numbers_mt(100000);
SELECT arraySort(groupArray(DISTINCT number % 13)) FROM numbers_mt(100000);
SELECT finalizeAggregation(countState(DISTINCT toString(number % 20))) FROM numbers_mt(100000);
-- SELECT corrStableDistinct(DISTINCT x, y) FROM (SELECT number % 11 AS x, number % 13 AS y FROM system.numbers LIMIT 1000);
