SELECT sum(DISTINCT x) FROM (SELECT number AS x FROM system.numbers_mt LIMIT 100000);
SELECT sum(DISTINCT x) FROM (SELECT number % 13 AS x FROM system.numbers_mt LIMIT 100000);
SELECT groupArray(DISTINCT x) FROM (SELECT number % 13 AS x FROM system.numbers_mt LIMIT 100000);
SELECT groupArray(DISTINCT x) FROM (SELECT number % 13 AS x FROM system.numbers_mt LIMIT 100000);
SELECT finalizeAggregation(countState(DISTINCT toString(number % 20))) FROM numbers_mt (100000);
-- SELECT corrStableDistinct(DISTINCT x, y) FROM (SELECT number % 11 AS x, number % 13 AS y FROM system.numbers LIMIT 1000);
