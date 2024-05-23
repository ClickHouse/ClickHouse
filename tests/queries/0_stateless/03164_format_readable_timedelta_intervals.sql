SELECT formatReadableTimeDelta(INTERVAL 1 NANOSECOND, 'seconds');
SELECT formatReadableTimeDelta(INTERVAL 1 MICROSECOND, 'seconds');
SELECT formatReadableTimeDelta(INTERVAL 1 MILLISECOND, 'seconds');
SELECT formatReadableTimeDelta(INTERVAL 1 SECOND, 'seconds');
SELECT formatReadableTimeDelta(INTERVAL 1 MINUTE, 'seconds');
SELECT formatReadableTimeDelta(INTERVAL 1 HOUR, 'seconds');
SELECT formatReadableTimeDelta(INTERVAL 1 DAY, 'seconds');
SELECT formatReadableTimeDelta(INTERVAL 1 WEEK, 'seconds');

-- MONTH, QUARTER & YEAR don't work as they don't represent constant intervals

SELECT formatReadableTimeDelta(INTERVAL 60 SECOND);
SELECT formatReadableTimeDelta(INTERVAL 3601 SECOND);
SELECT formatReadableTimeDelta(INTERVAL 61 MINUTE);
SELECT formatReadableTimeDelta(INTERVAL 3600 SECOND, 'minutes');

SELECT formatReadableTimeDelta(-INTERVAL 1 SECOND);
SELECT formatReadableTimeDelta(-INTERVAL 60 SECOND);
