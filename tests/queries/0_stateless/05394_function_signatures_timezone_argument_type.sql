-- A 2nd (timezone) argument of a wrong type is reported as a type mismatch,
-- not by the timezone extractor as `ILLEGAL_COLUMN`.

SELECT toStartOfDay(toDateTime('2020-01-01 10:20:30', 'UTC'), 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfHour(toDateTime('2020-01-01 10:20:30', 'UTC'), 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfMinute(toDateTime('2020-01-01 10:20:30', 'UTC'), 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toMonday(toDateTime('2020-01-01 10:20:30', 'UTC'), 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfMonth(toDateTime('2020-01-01 10:20:30', 'UTC'), 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- An empty timezone is still rejected.
SELECT toStartOfDay(toDateTime('2020-01-01 10:20:30', 'UTC'), ''); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfHour(toDateTime('2020-01-01 10:20:30', 'UTC'), ''); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toMonday(toDateTime('2020-01-01 10:20:30', 'UTC'), ''); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A valid timezone works.
SELECT toStartOfDay(toDateTime('2020-01-01 10:20:30', 'UTC'), 'Asia/Tokyo');
SELECT toStartOfHour(toDateTime('2020-01-01 10:20:30', 'UTC'), 'Asia/Tokyo');
SELECT toStartOfMinute(toDateTime('2020-01-01 10:20:30', 'UTC'), 'Asia/Tokyo');
SELECT toMonday(toDateTime('2020-01-01 10:20:30', 'UTC'), 'Asia/Tokyo');
SELECT toStartOfMonth(toDateTime('2020-01-01 10:20:30', 'UTC'), 'Asia/Tokyo');
