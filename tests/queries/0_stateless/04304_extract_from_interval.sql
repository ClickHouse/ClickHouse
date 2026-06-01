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

-- `date_part` form.
SELECT date_part('day',   INTERVAL 40 DAY);
SELECT date_part('month', INTERVAL 7 MONTH);

-- Lowercased keyword.
SELECT EXTRACT(year FROM INTERVAL 5 YEAR);

-- Zero and negative values pass through.
SELECT EXTRACT(DAY FROM INTERVAL 0 DAY);
SELECT EXTRACT(DAY FROM negate(INTERVAL 3 DAY));

-- Mismatched units are rejected.
SELECT EXTRACT(HOUR  FROM INTERVAL 5 DAY);   -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT EXTRACT(YEAR  FROM INTERVAL 5 DAY);   -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT EXTRACT(MONTH FROM INTERVAL 5 YEAR);  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT EXTRACT(DAY   FROM INTERVAL 5 MONTH); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Non-extractor functions sharing the same base class still reject Interval.
SELECT toStartOfYear(INTERVAL 5 YEAR); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
