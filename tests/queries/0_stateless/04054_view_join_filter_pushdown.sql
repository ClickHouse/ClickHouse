-- https://github.com/ClickHouse/ClickHouse/issues/67668
-- When querying a VIEW that contains a JOIN, the outer WHERE clause must be
-- pushed down through the JOIN to the underlying tables.
-- Tags: no-parallel-replicas

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS log_event;
DROP TABLE IF EXISTS data_user_utm;
DROP VIEW IF EXISTS view_event_all;

CREATE TABLE log_event
(
    `time` UInt64,
    `day` UInt32,
    `app_id` UInt32,
    `channel` String,
    `version` String,
    `guid` String
)
ENGINE = MergeTree
PARTITION BY (day, app_id)
ORDER BY (time, app_id, channel, version);

CREATE TABLE data_user_utm
(
    `app_id` UInt32,
    `guid` String,
    `utm_source` String,
    `active_time` UInt64,
    `utm_day` UInt32
)
ENGINE = ReplacingMergeTree
ORDER BY (app_id, guid);

CREATE VIEW view_event_all AS
SELECT log_event.*, data_user_utm.*
FROM log_event
ANY LEFT JOIN data_user_utm FINAL
ON log_event.app_id = data_user_utm.app_id AND log_event.guid = data_user_utm.guid;

-- Insert data into two partitions for each table
INSERT INTO log_event SELECT number, 20240725, 16, 'web', '1.0', toString(number) FROM numbers(100);
INSERT INTO log_event SELECT number, 20240726, 17, 'app', '2.0', toString(number) FROM numbers(100);
INSERT INTO data_user_utm SELECT 16, toString(number), 'google', number, 20240725 FROM numbers(100);
INSERT INTO data_user_utm SELECT 17, toString(number), 'facebook', number, 20240726 FROM numbers(100);

-- Verify data correctness through the view
SELECT count() FROM view_event_all WHERE app_id = 16 AND day = 20240725;

-- The outer WHERE clause should be pushed down and partition pruning should occur.
-- Without the fix, the left table reads all 2 parts; with it, only 1 part is read.
-- Extract Parts lines to verify filter pushdown works on both sides of the JOIN.
SELECT
    replaceRegexpOne(explain, '^\\s*ReadFromMergeTree \\(.*\\.(.*)\\)$', 'ReadFromMergeTree \\1') AS clean
FROM
(
    EXPLAIN indexes = 1
    SELECT *
    FROM view_event_all
    WHERE (app_id = 16) AND (day = 20240725) AND (time >= 0) AND (time <= 50)
    LIMIT 1
)
WHERE explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Parts: %' AND explain NOT LIKE '%Granules%');

DROP VIEW view_event_all;
DROP TABLE data_user_utm;
DROP TABLE log_event;
