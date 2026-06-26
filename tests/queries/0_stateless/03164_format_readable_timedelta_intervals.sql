SELECT formatReadableTimeDelta(INTERVAL 1 SECOND, 'seconds');
SELECT formatReadableTimeDelta(INTERVAL 1 MINUTE, 'seconds');
SELECT formatReadableTimeDelta(INTERVAL 1 HOUR, 'seconds');
SELECT formatReadableTimeDelta(INTERVAL 1 DAY, 'seconds');
SELECT formatReadableTimeDelta(INTERVAL 1 WEEK, 'seconds');

SELECT formatReadableTimeDelta(INTERVAL 1 NANOSECOND, 'nanoseconds');
SELECT formatReadableTimeDelta(INTERVAL 1 MICROSECOND, 'nanoseconds');
SELECT formatReadableTimeDelta(INTERVAL 1 MILLISECOND, 'nanoseconds');

-- MONTH, QUARTER and YEAR are not supported as they don't represent a fixed-sized interval in seconds.
SELECT formatReadableTimeDelta(INTERVAL 1 MONTH); -- { serverError BAD_ARGUMENTS }
SELECT formatReadableTimeDelta(INTERVAL 1 QUARTER); -- { serverError BAD_ARGUMENTS }
SELECT formatReadableTimeDelta(INTERVAL 1 YEAR); -- { serverError BAD_ARGUMENTS }

SELECT formatReadableTimeDelta(INTERVAL 60 SECOND);
SELECT formatReadableTimeDelta(INTERVAL 3601 SECOND);
SELECT formatReadableTimeDelta(INTERVAL 61 MINUTE);
SELECT formatReadableTimeDelta(INTERVAL 3600 SECOND, 'minutes');

SELECT formatReadableTimeDelta(-INTERVAL 1 SECOND);
SELECT formatReadableTimeDelta(-INTERVAL 60 SECOND);
