DROP TABLE IF EXISTS trend;
CREATE TABLE trend
(
    `event_date` Date,
    `user_id` Int32,
    `timestamp` DateTime,
    `eventID` Int32,
    `product` String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY user_id;

insert into trend values ('2019-01-28', 1, '2019-01-29 10:00:00', 1004, 'phone') ('2019-01-28', 1, '2019-01-29 10:00:00', 1003, 'phone') ('2019-01-28', 1, '2019-01-28 10:00:00', 1002, 'phone');

SELECT
    level,
    count() AS c
FROM
(
    SELECT
        user_id,
        windowFunnel(6048000000000000)(timestamp, eventID = 1004, eventID = 1003, eventID = 1002) AS level
    FROM trend
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC;

SELECT '---';

TRUNCATE TABLE trend;
insert into trend values ('2019-01-28', 1, '2019-01-29 10:00:00', 1003, 'phone') ('2019-01-28', 1, '2019-01-29 10:00:00', 1004, 'phone') ('2019-01-28', 1, '2019-01-28 10:00:00', 1002, 'phone');

SELECT
    level,
    count() AS c
FROM
(
    SELECT
        user_id,
        windowFunnel(6048000000000000)(timestamp, eventID = 1004, eventID = 1003, eventID = 1002) AS level
    FROM trend
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC;

DROP TABLE trend;
