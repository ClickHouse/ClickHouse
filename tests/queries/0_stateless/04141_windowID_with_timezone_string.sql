-- Regression for `windowID` with a timezone string as the 3rd argument:
-- it used to throw `LOGICAL_ERROR` / `ILLEGAL_COLUMN` because `extractIntervalKind`
-- was called unconditionally on the 3rd argument, which can be either an `Interval`
-- (hop) or a `String` (timezone).

SELECT windowID(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 DAY) > 0;
SELECT windowID(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 DAY, 'US/Samoa') > 0;
SELECT windowID(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 SECOND, 'US/Samoa') > 0;
SELECT windowID(toDateTime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 WEEK, 'US/Samoa') > 0;

-- The 4-argument form (hop_interval, window_interval, timezone) still requires both intervals to share the same kind.
SELECT windowID(toDateTime('2020-01-09 12:00:01'), INTERVAL 1 DAY, INTERVAL 1 SECOND, 'UTC'); -- { serverError ILLEGAL_COLUMN }
