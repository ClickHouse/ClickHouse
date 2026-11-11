-- Regression test for issue #103712:
-- toString(DateTime, Nullable(String)) must propagate NULL when the timezone column is NULL, not throw NOT_IMPLEMENTED

SET session_timezone = 'Europe/Amsterdam';

SELECT toString(toDateTime('2022-01-01 12:13:14'), CAST('UTC', 'Nullable(String)'));

SELECT toString(toDateTime('2022-01-01 12:13:14'), CAST(NULL, 'Nullable(String)'));

DROP TABLE IF EXISTS t_to_string_nullable_tz;
CREATE TABLE t_to_string_nullable_tz (id UInt32, tz Nullable(String)) ENGINE = Memory;
INSERT INTO t_to_string_nullable_tz VALUES (1, 'UTC'), (2, NULL), (3, 'Europe/Berlin');
SELECT id, toString(toDateTime('2022-01-01 12:13:14'), tz) FROM t_to_string_nullable_tz ORDER BY id;

SELECT id, toString(toDateTime64('2022-01-01 12:13:14.567', 3), tz) FROM t_to_string_nullable_tz ORDER BY id;

DROP TABLE IF EXISTS t_to_string_nullable_both;
CREATE TABLE t_to_string_nullable_both (id UInt32, dt Nullable(DateTime), tz Nullable(String)) ENGINE = Memory;
INSERT INTO t_to_string_nullable_both VALUES
    (1, toDateTime('2022-01-01 12:13:14'), 'UTC'),
    (2, toDateTime('2022-01-01 12:13:14'), NULL),
    (3, NULL, 'UTC'),
    (4, NULL, NULL);
SELECT id, toString(dt, tz) FROM t_to_string_nullable_both ORDER BY id;

DROP TABLE t_to_string_nullable_tz;
DROP TABLE t_to_string_nullable_both;
