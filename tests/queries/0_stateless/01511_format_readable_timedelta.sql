SELECT
    arrayJoin([1, 60, 60*60, 60*60*24, 60*60*24*30, 60*60*24*365]) AS elapsed,
    formatReadableTimeDelta(elapsed*5.5) AS time_delta;
SELECT
    'minutes' AS maximum_unit,
    arrayJoin([1, 60, 60*60, 60*60*24, 60*60*24*30, 60*60*24*365]) AS elapsed,
    formatReadableTimeDelta(elapsed*5.5, maximum_unit) AS time_delta;
SELECT
    'hours' AS maximum_unit,
    arrayJoin([1, 60, 60*60, 60*60*24, 60*60*24*30, 60*60*24*365]) AS elapsed,
    formatReadableTimeDelta(elapsed*5.5, maximum_unit) AS time_delta;
SELECT
    'days' AS maximum_unit,
    arrayJoin([1, 60, 60*60, 60*60*24, 60*60*24*30, 60*60*24*365]) AS elapsed,
    formatReadableTimeDelta(elapsed*5.5, maximum_unit) AS time_delta;
SELECT
    'months' AS maximum_unit,
    arrayJoin([1, 60, 60*60, 60*60*24, 60*60*24*30, 60*60*24*365]) AS elapsed,
    formatReadableTimeDelta(elapsed*5.5, maximum_unit) AS time_delta;
