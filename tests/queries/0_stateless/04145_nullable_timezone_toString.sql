-- Regression test for toString(DateTime, Nullable(String)) where the timezone
-- column contains NULL. Before the fix this threw:
--   Code: 48. NOT_IMPLEMENTED: Method getDataAt is not supported for Nullable(String)
-- NULL timezone should propagate to NULL in the result.

SET session_timezone = 'Europe/Amsterdam';

-- Non-NULL value in Nullable(String) timezone must work (the case fixed by PR #59190).
SELECT toString(toDateTime('2022-01-01 12:13:14'), CAST('UTC', 'Nullable(String)'));

-- NULL value in Nullable(String) timezone must propagate NULL.
SELECT toString(toDateTime('2022-01-01 12:13:14'), CAST(NULL, 'Nullable(String)'));

-- Table-based test: both NULL and non-NULL rows in the timezone column.
DROP TABLE IF EXISTS t_nullable_tz;
CREATE TABLE t_nullable_tz (tz Nullable(String)) ENGINE = Memory;
INSERT INTO t_nullable_tz VALUES ('UTC'), (NULL);
SELECT toString(toDateTime('2022-01-01 12:13:14'), tz) FROM t_nullable_tz ORDER BY tz NULLS LAST;
DROP TABLE t_nullable_tz;

-- Table-based test: both datetime and timezone are Nullable.
DROP TABLE IF EXISTS t_both_nullable;
CREATE TABLE t_both_nullable (dt Nullable(DateTime), tz Nullable(String)) ENGINE = Memory;
INSERT INTO t_both_nullable VALUES ('2022-01-01 12:13:14', 'UTC'), (NULL, 'UTC'), ('2022-01-01 12:13:14', NULL), (NULL, NULL);
SELECT toString(dt, tz) FROM t_both_nullable ORDER BY dt NULLS LAST, tz NULLS LAST;
DROP TABLE t_both_nullable;
