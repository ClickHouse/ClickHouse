-- Tests EXTRACT(<unit> FROM INTERVAL ...) and date_part on Interval values.
-- ClickHouse intervals are single-kind, so the unit MUST match the interval's
-- kind. Mismatched kinds are rejected (strict semantics).
-- Issue: https://github.com/ClickHouse/ClickHouse/issues/105956

-- Same-kind extraction returns the underlying value.
SELECT EXTRACT(YEAR        FROM INTERVAL 5 YEAR);
SELECT EXTRACT(QUARTER     FROM INTERVAL 4 QUARTER);
SELECT EXTRACT(MONTH       FROM INTERVAL 7 MONTH);
SELECT EXTRACT(WEEK        FROM INTERVAL 52 WEEK);
SELECT EXTRACT(DAY         FROM INTERVAL 40 DAY);
SELECT EXTRACT(HOUR        FROM INTERVAL 23 HOUR);
SELECT EXTRACT(MINUTE      FROM INTERVAL 59 MINUTE);
SELECT EXTRACT(SECOND      FROM INTERVAL 12 SECOND);
SELECT EXTRACT(MILLISECOND FROM INTERVAL 500 MILLISECOND);
SELECT EXTRACT(MICROSECOND FROM INTERVAL 750 MICROSECOND);
SELECT EXTRACT(NANOSECOND  FROM INTERVAL 900 NANOSECOND);

-- `date_part` form.
SELECT date_part('day',   INTERVAL 40 DAY);
SELECT date_part('month', INTERVAL 7 MONTH);

-- Lowercased keyword.
SELECT EXTRACT(year FROM INTERVAL 5 YEAR);

-- Zero and negative values pass through.
SELECT EXTRACT(DAY FROM INTERVAL 0 DAY);
SELECT EXTRACT(DAY FROM negate(INTERVAL 3 DAY));

-- Mismatched units are rejected, both for constant and non-constant interval
-- expressions (validated at type-analysis time, not just at execution).
SELECT EXTRACT(HOUR  FROM INTERVAL 5 DAY);   -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT EXTRACT(YEAR  FROM INTERVAL 5 DAY);   -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT EXTRACT(MONTH FROM INTERVAL 5 YEAR);  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT EXTRACT(DAY   FROM INTERVAL 5 MONTH); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT EXTRACT(HOUR  FROM materialize(INTERVAL 5 DAY)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT EXTRACT(DAY FROM toIntervalHour(number)) FROM numbers(0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Sub-second extraction also works on DateTime64.
SELECT EXTRACT(MILLISECOND FROM toDateTime64('2024-01-01 10:20:30.123456789', 9));
SELECT EXTRACT(MICROSECOND FROM toDateTime64('2024-01-01 10:20:30.123456789', 9));
SELECT EXTRACT(NANOSECOND  FROM toDateTime64('2024-01-01 10:20:30.123456789', 9));

-- Non-extractor functions sharing the same base class still reject Interval.
SELECT toStartOfYear(INTERVAL 5 YEAR); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
