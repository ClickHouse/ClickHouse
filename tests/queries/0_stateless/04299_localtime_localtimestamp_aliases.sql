-- LOCALTIMESTAMP is a SQL-standard / PostgreSQL alias for now() (returns DateTime).
-- LOCALTIME is the SQL-standard / PostgreSQL time-of-day function (returns Time), equal to CAST(now() AS Time).

-- Omitting parentheses for niladic functions is only supported by the analyzer.
SET enable_analyzer = 1;

-- LOCALTIMESTAMP has the same result type as now().
SELECT toTypeName(LOCALTIMESTAMP) = toTypeName(now());

-- LOCALTIME returns Time.
SELECT toTypeName(LOCALTIME);

-- The LOCALTIMESTAMP value matches now() within a tolerance. now() reads the clock per call,
-- so two separate now()-family expressions may straddle a one-second boundary.
SELECT abs(toInt64(LOCALTIMESTAMP) - toInt64(now())) <= 1;

-- The LOCALTIME value matches CAST(now() AS Time) within a tolerance. Both are time-of-day
-- values in [0, 86400), and LOCALTIME and now() read the clock independently, so near midnight
-- one may have wrapped past 00:00:00 while the other has not. Compare the circular distance
-- modulo 86400 (the difference is computed once in the subquery, so both terms see the same value).
SELECT least(d, 86400 - d) <= 1 FROM (SELECT abs(toInt64(LOCALTIME) - toInt64(CAST(now() AS Time))) AS d);

-- The functions are case-insensitive, like current_timestamp.
SELECT toTypeName(localtimestamp) = toTypeName(now());
SELECT toTypeName(LocalTimeStamp) = toTypeName(now());
SELECT toTypeName(localtime) = 'Time';
SELECT toTypeName(LocalTime) = 'Time';
