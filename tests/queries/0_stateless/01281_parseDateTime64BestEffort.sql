-- Error cases
SELECT parseDateTime64BestEffort();  -- {serverError 42}
SELECT parseDateTime64BestEffort(123);  -- {serverError 43}
SELECT parseDateTime64BestEffort('foo'); -- {serverError 41}

SELECT parseDateTime64BestEffort('2020-05-14T03:37:03.253184Z', 'bar');  -- {serverError 43} -- invalid scale parameter
SELECT parseDateTime64BestEffort('2020-05-14T03:37:03.253184Z', 3, 4);  -- {serverError 43} -- invalid timezone parameter
SELECT parseDateTime64BestEffort('2020-05-14T03:37:03.253184Z', 3, 'baz');  -- {serverError 1000} -- unknown timezone

SELECT parseDateTime64BestEffort('2020-05-14T03:37:03.253184Z', materialize(3), 4);  -- {serverError 44} -- non-const precision
SELECT parseDateTime64BestEffort('2020-05-14T03:37:03.253184Z', 3, materialize('UTC'));  -- {serverError 44} -- non-const timezone

SELECT parseDateTime64BestEffort('2020-05-14T03:37:03.253184012345678910111213141516171819Z', 3, 'UTC'); -- {serverError 6}

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