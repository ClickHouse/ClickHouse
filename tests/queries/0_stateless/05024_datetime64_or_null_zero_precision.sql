SET session_timezone = 'America/New_York';

SELECT toTypeName(toDateTime64OrNull('1789-07-14', 0));
SELECT toDateTime64OrNull('1789-07-14', 0);
SELECT toDateTime64OrNull('0001-01-01', 0);

SELECT toTypeName(toDateTime64OrNull('1789-07-14', 0, 'UTC'));
SELECT toDateTime64OrNull('1789-07-14', 0, 'UTC');
SELECT toDateTime64OrNull('0001-01-01', 0, 'UTC');
SELECT toDateTime64OrNull('1969-12-31 23:59:59', 0, 'UTC');
SELECT toDateTime64OrNull(toInt64(-1), 0, 'UTC');
SELECT toDateTime64OrNull('invalid', 0, 'UTC');

SELECT toTypeName(toDateTime64OrZero('1789-07-14', 0, 'UTC'));
SELECT toDateTime64OrZero('1789-07-14', 0, 'UTC');
SELECT toDateTime64OrZero('0001-01-01', 0, 'UTC');

-- The zero-precision result type is shared with the parseDateTime64BestEffort* family.
SELECT toTypeName(parseDateTime64BestEffort('1789-07-14', 0, 'UTC'));
SELECT parseDateTime64BestEffort('1789-07-14', 0, 'UTC');
SELECT toTypeName(parseDateTime64BestEffortOrNull('1789-07-14', 0, 'UTC'));
SELECT parseDateTime64BestEffortOrNull('1789-07-14', 0, 'UTC');
SELECT toTypeName(parseDateTime64BestEffortUSOrZero('07/14/1789', 0, 'UTC'));
SELECT parseDateTime64BestEffortUSOrZero('07/14/1789', 0, 'UTC');
