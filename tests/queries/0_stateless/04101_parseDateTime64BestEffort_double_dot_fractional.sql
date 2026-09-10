set session_timezone='UTC';

SELECT parseDateTime64BestEffort('12:34:56..123'); -- {serverError CANNOT_PARSE_DATETIME}
SELECT parseDateTime64BestEffortOrNull('12:34:56..123');
SELECT parseDateTime64BestEffortOrZero('12:34:56..123');

SELECT parseDateTime64BestEffort('12:34:56.123.456'); -- {serverError CANNOT_PARSE_DATETIME}
SELECT parseDateTime64BestEffortOrNull('12:34:56.123.456');
SELECT parseDateTime64BestEffortOrZero('12:34:56.123.456');

SELECT parseDateTime64BestEffort('12:34:56.123456789012345678.987654321'); -- {serverError CANNOT_PARSE_DATETIME}
SELECT parseDateTime64BestEffortOrNull('12:34:56.123456789012345678.987654321');
SELECT parseDateTime64BestEffortOrZero('12:34:56.123456789012345678.987654321');
