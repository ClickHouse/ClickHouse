SELECT
    toDateTimeOrNull('0000-00-00 00:00:00', 'UTC'),
    toDateTimeOrNull('0000-01-00 00:00:00', 'UTC'),
    toDateTimeOrNull('0000-00-01 00:00:00', 'UTC'),
    toDateTimeOrNull('0000-01-01 00:00:00', 'UTC');

SELECT toDateTime('0000-01-01 00:00:00', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

SELECT
    parseDateTimeBestEffortOrNull('0000-00-00 00:00:00', 'UTC'),
    parseDateTimeBestEffortOrNull('0000-01-00 00:00:00', 'UTC'),
    parseDateTimeBestEffortOrNull('0000-00-01 00:00:00', 'UTC'),
    parseDateTimeBestEffortOrNull('0000-01-01 00:00:00', 'UTC'),
    parseDateTimeBestEffortOrNull('0000', 'UTC'),
    parseDateTimeBestEffortOrNull('0000-05', 'UTC');

SELECT
    parseDateTime64BestEffortOrNull('0000-00-00 00:00:00', 3, 'UTC'),
    parseDateTime64BestEffortOrNull('0000-01-00 00:00:00', 3, 'UTC'),
    parseDateTime64BestEffortOrNull('0000-00-01 00:00:00', 3, 'UTC'),
    parseDateTime64BestEffortOrNull('0000-01-01 00:00:00', 3, 'UTC'),
    parseDateTime64BestEffortOrNull('0000', 3, 'UTC'),
    parseDateTime64BestEffortOrNull('0000-05', 3, 'UTC');

SELECT *
FROM format(TSV, 't DateTime(\'UTC\')', '0000-01-00 00:00:00')
SETTINGS date_time_input_format = 'basic';

SELECT *
FROM format(TSV, 't DateTime(\'UTC\')', '0000-01-01 00:00:00')
SETTINGS date_time_input_format = 'basic'; -- { serverError CANNOT_PARSE_DATETIME }

SELECT *
FROM format(TSV, 't DateTime64(3, \'UTC\')', '0000-01-00 00:00:00')
SETTINGS date_time_input_format = 'basic';

SELECT *
FROM format(TSV, 't DateTime64(3, \'UTC\')', '0000-01-01 00:00:00')
SETTINGS date_time_input_format = 'basic';
