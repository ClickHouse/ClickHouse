CREATE TABLE t
(
    `timestamp` DateTime,
    `id` String,
    `value` UInt8,
)
ENGINE = MergeTree
ORDER BY (id, toStartOfDay(timestamp))
TTL timestamp + toIntervalDay(1)
GROUP BY id, toStartOfDay(timestamp)
SET id = argMax(id, timestamp), value = argMax(value, timestamp), timestamp + toIntervalDay(10);

INSERT INTO t VALUES (parseDateTimeBestEffort('2025-06-9 10:00'), 'pepe', 1);
INSERT INTO t VALUES (parseDateTimeBestEffort('2025-06-10 10:00'), 'pepe', 2);
INSERT INTO t VALUES (parseDateTimeBestEffort('2025-06-10 11:00'), 'pepe', 3);

OPTIMIZE TABLE t FINAL;
SELECT '-- TTL GROUP BY is applied';
SELECT * FROM t;

INSERT INTO t VALUES (parseDateTimeBestEffort('2025-05-10 11:00'), 'pepe', 4);
OPTIMIZE TABLE t FINAL;
SELECT '-- TTL removes old elements';
SELECT * FROM t;
