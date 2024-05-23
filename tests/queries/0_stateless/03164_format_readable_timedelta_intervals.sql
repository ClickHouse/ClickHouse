SELECT formatReadableTimeDelta(INTERVAL 1 SECOND);
SELECT formatReadableTimeDelta(INTERVAL 60 SECOND);
SELECT formatReadableTimeDelta(INTERVAL 3601 SECOND);
SELECT formatReadableTimeDelta(INTERVAL 61 MINUTE);

SELECT formatReadableTimeDelta(INTERVAL 3600 SECOND, 'minutes');
SELECT formatReadableTimeDelta(-INTERVAL 1 SECOND);
SELECT formatReadableTimeDelta(-INTERVAL 60 SECOND);