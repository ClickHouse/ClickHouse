-- The `Field` path used by `INSERT ... VALUES` must agree with the column path used by `CAST`
-- for every `Time` / `Time64` -> whole-second temporal conversion, and scale-zero `Time` must
-- behave exactly like `Time64(0)` of the same value.

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

-- Scale-zero `Time`: the `Field` path and the `CAST` column path must produce the same value, and
-- the same one `Time64(0)` produces for that value.
CREATE TABLE time_values_negative (datetime DateTime('UTC')) ENGINE = Memory;

INSERT INTO time_values_negative VALUES (CAST(-1 AS Time));
INSERT INTO time_values_negative VALUES (CAST(3661 AS Time));
SELECT * FROM time_values_negative ORDER BY ALL FORMAT TSV;

SELECT CAST(CAST(-1 AS Time) AS DateTime('UTC')), CAST(CAST(3661 AS Time) AS DateTime('UTC')) FORMAT TSV;
SELECT CAST(CAST(-1 AS Time) AS DateTime('UTC')) = CAST(CAST(-1 AS Time64(0)) AS DateTime('UTC')) FORMAT TSV;

DROP TABLE time_values_negative;

-- `IN` is the entry point that actually reaches the `convertFieldToType` branches for these
-- conversions: a `VALUES` expression is evaluated with the target type already known, so it never
-- needs a `Field` conversion. Every `Time` / `Time64` -> whole-second target must be accepted here;
-- `DateTime` <- `Time` was the one missing combination and reported `TYPE_MISMATCH`.
SELECT 'in_datetime_time', toDateTime('1970-01-01 01:01:01', 'UTC') IN (CAST(3661 AS Time)) FORMAT TSV;
SELECT 'in_datetime_time64', toDateTime('1970-01-01 01:01:01', 'UTC') IN (CAST(3661 AS Time64(0))) FORMAT TSV;
SELECT 'in_datetime64_time', toDateTime64('1970-01-01 01:01:01', 3, 'UTC') IN (CAST(3661 AS Time)) FORMAT TSV;
SELECT 'in_date_time', toDate('1970-01-02') IN (CAST(86400 AS Time)) FORMAT TSV;
SELECT 'in_date32_time', toDate32('1970-01-02') IN (CAST(86400 AS Time)) FORMAT TSV;

-- A negative time-of-day is not exactly representable as `DateTime`, so the strict `IN`
-- conversion excludes it from the set instead of wrapping it to the end of the range,
-- identically for both sources.
SELECT 'in_neg_time', toDateTime('2106-02-07 06:28:15', 'UTC') IN (CAST(-1 AS Time)) FORMAT TSV;
SELECT 'in_neg_time64', toDateTime('2106-02-07 06:28:15', 'UTC') IN (CAST(-1 AS Time64(0))) FORMAT TSV;

-- The two sources stay in step whatever `date_time_overflow_behavior` says - the strict `IN`
-- conversion rejects the value before any overflow handling - so what is pinned here is that
-- `Time` follows `Time64` rather than any particular overflow semantics.
SET date_time_overflow_behavior = 'saturate';
SELECT 'in_saturate_agree', (toDateTime('2106-02-07 06:28:15', 'UTC') IN (CAST(-1 AS Time)))
    = (toDateTime('2106-02-07 06:28:15', 'UTC') IN (CAST(-1 AS Time64(0)))) FORMAT TSV;

SET date_time_overflow_behavior = 'throw';
SELECT 'in_throw_agree', (toDateTime('2106-02-07 06:28:15', 'UTC') IN (CAST(-1 AS Time)))
    = (toDateTime('2106-02-07 06:28:15', 'UTC') IN (CAST(-1 AS Time64(0)))) FORMAT TSV;
