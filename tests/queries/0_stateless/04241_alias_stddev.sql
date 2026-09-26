-- STDDEV is a PostgreSQL/SQL-standard alias of stddevSamp (sample standard deviation),
-- matching the behavior of PostgreSQL, Oracle, BigQuery, Snowflake, and DuckDB.
SELECT STDDEV(number) = stddevSamp(number) FROM numbers(10);
SELECT stddev(number) = stddevSamp(number) FROM numbers(10);
SELECT Stddev(number) = stddevSamp(number) FROM numbers(10);
