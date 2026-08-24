
DROP TABLE IF EXISTS log;

CREATE TABLE log(
    collectorReceiptTime DateTime,
    eventId String,
    ruleId Nullable(String),
    PROJECTION ailog_rule_count (
    SELECT
        collectorReceiptTime,
        ruleId,
        count(ruleId)
    GROUP BY
        collectorReceiptTime,
        ruleId
    )
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(collectorReceiptTime)
ORDER BY (collectorReceiptTime, eventId);

INSERT INTO log VALUES ('2025-01-01 00:02:03', 'eventId_001', Null);
INSERT INTO log VALUES ('2025-01-01 01:04:05', 'eventId_002', Null);
INSERT INTO log VALUES ('2025-01-01 02:06:07', 'eventId_003', Null);
INSERT INTO log VALUES ('2025-01-01 03:08:09', 'eventId_004', Null);
INSERT INTO log VALUES ('2025-01-01 04:10:11', 'eventId_005', Null);
INSERT INTO log VALUES ('2025-01-01 05:12:13', 'eventId_006', Null);

SET parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;

SELECT
    formatDateTime(toStartOfInterval(collectorReceiptTime, toIntervalHour(1)), '%Y-%m-%d %H') AS time,
    COUNT() AS count
FROM log
WHERE (collectorReceiptTime >= '2025-01-01 00:00:00') AND (collectorReceiptTime <= '2025-01-01 23:59:59')
GROUP BY time
ORDER BY time DESC;

-- Another similar case to verify that COUNT(NOT NULL) should be able to use aggregate projection.

DROP TABLE log;

CREATE TABLE log(
    collectorReceiptTime DateTime,
    eventId String,
    ruleId String,
    PROJECTION ailog_rule_count (
    SELECT
        collectorReceiptTime,
        ruleId,
        count(ruleId)
    GROUP BY
        collectorReceiptTime,
        ruleId
    )
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(collectorReceiptTime)
ORDER BY (collectorReceiptTime, eventId);

INSERT INTO log VALUES ('2025-01-01 00:02:03', 'eventId_001', '');
INSERT INTO log VALUES ('2025-01-01 01:04:05', 'eventId_002', '');
INSERT INTO log VALUES ('2025-01-01 02:06:07', 'eventId_003', '');
INSERT INTO log VALUES ('2025-01-01 03:08:09', 'eventId_004', '');
INSERT INTO log VALUES ('2025-01-01 04:10:11', 'eventId_005', '');
INSERT INTO log VALUES ('2025-01-01 05:12:13', 'eventId_006', '');

SELECT
    formatDateTime(toStartOfInterval(collectorReceiptTime, toIntervalHour(1)), '%Y-%m-%d %H') AS time,
    COUNT() AS count
FROM log
WHERE (collectorReceiptTime >= '2025-01-01 00:00:00') AND (collectorReceiptTime <= '2025-01-01 23:59:59')
GROUP BY time
ORDER BY time DESC
SETTINGS force_optimize_projection = 1;

DROP TABLE log;
