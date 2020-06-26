SELECT sum(DISTINCT number % 13) FROM remote('127.0.0.{1,2}', numbers_mt(100000));
SELECT arraySort(groupArray(DISTINCT number % 13)) FROM remote('127.0.0.{1,2}', numbers_mt(100000));
SELECT finalizeAggregation(countState(DISTINCT toString(number % 20))) FROM remote('127.0.0.{1,2}', numbers_mt(100000));
SELECT round(corrStable(DISTINCT x, y), 5) FROM (SELECT number % 10 AS x, number % 5 AS y FROM remote('127.0.0.{1,2}', numbers(1000)));
