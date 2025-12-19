-- Tags: no-parallel

CREATE TABLE t
(
    `timestamp` DateTime,
    `id` String,
    `value` UInt16,
)
ENGINE = MergeTree
ORDER BY (id, toStartOfDay(timestamp))
TTL timestamp + toIntervalDay(1)
    GROUP BY id, toStartOfDay(timestamp)
    SET timestamp = max(timestamp), id = argMax(id, timestamp), value = max(value);

INSERT INTO t VALUES (parseDateTimeBestEffort('2000-06-9 10:00'), 'pepe', 1000);
INSERT INTO t VALUES (parseDateTimeBestEffort('2000-06-10 10:00'), 'pepe', 1000);

-- Inserts the maximum value, but with an older timestmap. The value should be taken in the aggregation.
INSERT INTO t VALUES (parseDateTimeBestEffort('2000-06-10 11:00'), 'pepe', 11000);
INSERT INTO t VALUES (parseDateTimeBestEffort('2000-06-10 12:00'), 'pepe', 1200);

-- Inserts the latest timestamp, which should be the one taken in the aggregation.
INSERT INTO t VALUES (parseDateTimeBestEffort('2000-06-10 13:00'), 'pepe', 1300);

OPTIMIZE TABLE t FINAL;
SELECT '-- Intersecting columns in GROUP BY and SET';
SELECT * FROM t ORDER BY ALL;

REPLACE TABLE t
(
    `timestamp` DateTime,
    `id` String,
    `value` String,
)
ENGINE = MergeTree
ORDER BY (id, toStartOfDay(timestamp))
TTL timestamp + toIntervalDay(1)
    GROUP BY id, toStartOfDay(timestamp)
    SET timestamp = max(timestamp), id = max(value);

INSERT INTO t VALUES (parseDateTimeBestEffort('2000-06-9 10:00'), 'pepe', 'a');
INSERT INTO t VALUES (parseDateTimeBestEffort('2000-06-10 10:00'), 'pepe', 'b');
INSERT INTO t VALUES (parseDateTimeBestEffort('2000-06-10 11:00'), 'pepe', 'c');

OPTIMIZE TABLE t FINAL;
SELECT '-- Intersecting columns in GROUP AND SET where SET is prioritized';
SELECT * FROM t ORDER BY ALL;
