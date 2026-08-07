CREATE TEMPORARY TABLE sessions (date DateTime, visitorId String, sessionId String);
CREATE TEMPORARY TABLE orders (date DateTime, visitorId String, orderId String);

INSERT INTO sessions VALUES ('2018-01-01 00:00:00', 'v1', 's1'), ('2018-01-02 00:00:00', 'v1', 's2'), ('2018-01-03 00:00:00', 'v2', 's3'), ('2018-01-04 00:00:00', 'v1', 's4'), ('2018-01-05 00:00:00', 'v2', 's5'), ('2018-01-06 00:00:00', 'v3', 's6');
INSERT INTO orders VALUES ('2018-01-03 00:00:00', 'v1', 'o1'), ('2018-01-05 00:00:00', 'v1', 'o2'), ('2018-01-06 00:00:00', 'v2', 'o3');

SELECT
    visitorId,
    orderId,
    groupUniqArray(sessionId)
FROM sessions
ASOF INNER JOIN orders ON (sessions.visitorId = orders.visitorId) AND (sessions.date <= orders.date)
GROUP BY
    visitorId,
    orderId
ORDER BY
    visitorId ASC,
    orderId ASC;

SELECT
    visitorId,
    orderId,
    groupUniqArray(sessionId)
FROM sessions
ASOF INNER JOIN orders ON (sessions.visitorId = orders.visitorId) AND (sessions.date <= orders.date)
GROUP BY
    visitorId,
    orderId
ORDER BY
    visitorId ASC,
    orderId ASC
SETTINGS join_algorithm = 'full_sorting_merge';
