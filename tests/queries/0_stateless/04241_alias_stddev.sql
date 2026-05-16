-- STDDEV is a PostgreSQL/SQL-standard alias of stddevPop.
SELECT STDDEV(number) = stddevPop(number) FROM numbers(10);
SELECT stddev(number) = stddevPop(number) FROM numbers(10);
SELECT Stddev(number) = stddevPop(number) FROM numbers(10);
