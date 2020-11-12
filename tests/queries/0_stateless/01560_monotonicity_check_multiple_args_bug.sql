WITH arrayJoin(range(2)) AS delta
SELECT
    toDate(time) + toIntervalDay(delta) AS dt,
    version()
FROM 
(
    SELECT NOW() AS time
)
ORDER BY dt ASC;

WITH arrayJoin([0, 1]) AS delta
SELECT
    toDate(time) + toIntervalDay(delta) AS dt,
    version()
FROM 
(
    SELECT NOW() AS time
)
ORDER BY dt ASC;
