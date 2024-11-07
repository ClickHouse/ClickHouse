-- Error cases
SELECT parseDateTime64BestEffort();  -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT parseDateTime64BestEffort(123);  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT parseDateTime64BestEffort('foo'); -- {serverError CANNOT_PARSE_DATETIME}

SELECT parseDateTime64BestEffort('2020-05-14T03:37:03.253184Z', 'bar');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT} -- invalid scale parameter
SELECT parseDateTime64BestEffort('2020-05-14T03:37:03.253184Z', 3, 4);  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT} -- invalid timezone parameter
SELECT parseDateTime64BestEffort('2020-05-14T03:37:03.253184Z', 3, 'baz');  -- {serverError BAD_ARGUMENTS} -- unknown timezone

SELECT parseDateTime64BestEffort('2020-05-14T03:37:03.253184Z', materialize(3), 4);  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT, 44} -- non-const precision
SELECT parseDateTime64BestEffort('2020-05-14T03:37:03.253184Z', 3, materialize('UTC'));  -- {serverError ILLEGAL_COLUMN} -- non-const timezone

SELECT parseDateTime64BestEffort('2020-05-14T03:37:03.253184012345678910111213141516171819Z', 3, 'UTC'); -- {serverError CANNOT_PARSE_TEXT}

SELECT 'orNull';
SELECT parseDateTime64BestEffortOrNull('2020-05-14T03:37:03.253184Z', 3, 'UTC');
SELECT parseDateTime64BestEffortOrNull('foo', 3, 'UTC');

SELECT 'orZero';
SELECT parseDateTime64BestEffortOrZero('2020-05-14T03:37:03.253184Z', 3, 'UTC');
SELECT parseDateTime64BestEffortOrZero('bar', 3, 'UTC');

SELECT 'non-const';
SELECT parseDateTime64BestEffort(materialize('2020-05-14T03:37:03.253184Z'), 3, 'UTC');

SELECT 'Timezones';
SELECT parseDateTime64BestEffort('2020-05-14T03:37:03.253184Z', 3, 'UTC');
SELECT parseDateTime64BestEffort('2020-05-14T03:37:03.253184Z', 3, 'Europe/Minsk');

SELECT 'Formats';
SELECT parseDateTime64BestEffort('2020-05-14T03:37:03.253184', 3, 'UTC');
SELECT parseDateTime64BestEffort('2020-05-14T03:37:03', 3, 'UTC');
SELECT parseDateTime64BestEffort('2020-05-14 03:37:03', 3, 'UTC');

SELECT 'Unix Timestamp with Milliseconds';
SELECT parseDateTime64BestEffort('1640649600123', 3, 'UTC');
SELECT parseDateTime64BestEffort('1640649600123', 1, 'UTC');
SELECT parseDateTime64BestEffort('1640649600123', 6, 'UTC');
