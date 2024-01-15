-- max_unit bigger than second, min_unit omitted (and considered 'seconds')
WITH
    'hours' AS maximum_unit,
    arrayJoin([1.12, 60.2, 123.33, 24.45, 35.57, 66.64, 67.79, 48.88, 99.96, 3600]) AS elapsed
SELECT
    formatReadableTimeDelta(elapsed, maximum_unit) AS time_delta;

-- max_unit smaller than second, min_unit omitted (and considered 'nanoseconds')
WITH
    'milliseconds' AS maximum_unit,
    arrayJoin([1.12, 60.2, 123.33, 24.45, 35.57, 66.64, 67.79797979, 48.888888, 99.96, 3600]) AS elapsed
SELECT
    formatReadableTimeDelta(elapsed, maximum_unit) AS time_delta;

-- Check exception is thrown
SELECT formatReadableTimeDelta(1.1, 'seconds', 'hours'); -- { serverError BAD_ARGUMENTS }

-- Check empty units are omitted unless they are the only one
WITH
    'hours' AS maximum_unit,
    'microseconds' as minimum_unit,
    arrayJoin([0, 3601.000000003]) AS elapsed
SELECT
    formatReadableTimeDelta(elapsed, maximum_unit, minimum_unit);

WITH
    'milliseconds' AS maximum_unit,
    arrayJoin([0, 1.0005]) AS elapsed
SELECT
    formatReadableTimeDelta(elapsed, maximum_unit);
