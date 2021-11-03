WITH arrayJoin(range(2)) AS delta
SELECT
    toDate(time) + toIntervalDay(delta) AS dt
FROM 
(
    SELECT toDateTime('2020.11.12 19:02:04') AS time
)
ORDER BY dt ASC;

WITH arrayJoin([0, 1]) AS delta
SELECT
    toDate(time) + toIntervalDay(delta) AS dt
FROM 
(
    SELECT toDateTime('2020.11.12 19:02:04') AS time
)
ORDER BY dt ASC;
