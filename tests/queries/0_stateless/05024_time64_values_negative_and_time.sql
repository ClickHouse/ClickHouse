-- The `Field` path used by `INSERT ... VALUES` must agree with the column path used by `CAST`
-- and `INSERT ... SELECT` for every `Time64` -> whole-second temporal conversion.

SET allow_experimental_time_time64_type = 1;
SET session_timezone = 'America/New_York';

CREATE TABLE time64_values_negative
(
    datetime DateTime('UTC'),
    time_col Time
) ENGINE = Memory;

-- A negative time-of-day wraps into the end of the `DateTime` range under the default
-- `date_time_overflow_behavior = 'ignore'`, and is kept as-is by `Time`.
INSERT INTO time64_values_negative VALUES (CAST(-1 AS Time64(0)), toTime64('-01:02:03.456', 3));

-- The fractional part is floored towards negative infinity for `Time`, exactly as `CAST` does.
INSERT INTO time64_values_negative VALUES (toTime64('01:02:03.456', 3), toTime64('01:02:03.456', 3));

SELECT * FROM time64_values_negative ORDER BY ALL FORMAT TSV;

-- The same values through the column path must produce the same rows.
SELECT CAST(CAST(-1 AS Time64(0)) AS DateTime('UTC')), CAST(toTime64('-01:02:03.456', 3) AS Time) FORMAT TSV;
SELECT CAST(toTime64('01:02:03.456', 3) AS DateTime('UTC')), CAST(toTime64('01:02:03.456', 3) AS Time) FORMAT TSV;

DROP TABLE time64_values_negative;
