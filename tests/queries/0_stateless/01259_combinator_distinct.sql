SELECT sum(DISTINCT number) FROM numbers_mt(100000);
SELECT sum(DISTINCT number % 13) FROM numbers_mt(100000);
SELECT arraySort(groupArray(DISTINCT number % 13)) FROM numbers_mt(100000);
SELECT finalizeAggregation(countState(DISTINCT toString(number % 20))) FROM numbers_mt(100000);
SELECT round(corrStable(DISTINCT x, y), 5) FROM (SELECT number % 10 AS x, number % 5 AS y FROM numbers(1000));
SELECT round(corrStable(x, y), 5) FROM (SELECT DISTINCT number % 10 AS x, number % 5 AS y FROM numbers(1000));

SELECT sum(DISTINCT y) FROM (SELECT number % 5 AS x, number % 15 AS y FROM numbers(1000)) GROUP BY x;

SELECT countIf(DISTINCT number % 10, number % 5 = 2) FROM numbers(10000);
EXPLAIN SYNTAX SELECT countIf(DISTINCT number % 10, number % 5 = 2) FROM numbers(10000);

SELECT sumIf(DISTINCT number % 10, number % 5 = 2) FROM numbers(10000);
EXPLAIN SYNTAX SELECT sumIf(DISTINCT number % 10, number % 5 = 2) FROM numbers(10000);
