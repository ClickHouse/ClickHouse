-- https://github.com/ClickHouse/ClickHouse/issues/48308
SET enable_analyzer=1;
DROP TABLE IF EXISTS 03038_table;

CREATE TABLE 03038_table
(
    `time` DateTime
)
ENGINE = MergeTree
ORDER BY time;

SELECT *
FROM
(
    SELECT
        toUInt64(time) AS time,
        toHour(03038_table.time)
    FROM 03038_table
)
ORDER BY time ASC;

WITH subquery AS (
    SELECT
        toUInt64(time) AS time,
        toHour(03038_table.time)
    FROM 03038_table
)
SELECT *
FROM subquery
ORDER BY subquery.time ASC;

SELECT *
FROM
(
    SELECT
        toUInt64(time) AS time,
        toHour(03038_table.time) AS hour
    FROM 03038_table
)
ORDER BY time ASC, hour;

DROP TABLE IF EXISTS 03038_table;
