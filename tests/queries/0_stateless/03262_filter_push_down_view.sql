DROP TABLE IF EXISTS alpha;
DROP TABLE IF EXISTS alpha__day;

SET session_timezone = 'Etc/UTC';

CREATE TABLE alpha
(
    `ts` DateTime64(6),
    `auid` Int64,
)
ENGINE = MergeTree
ORDER BY (auid, ts)
SETTINGS index_granularity = 1;

CREATE VIEW alpha__day
(
    `ts_date` Date,
    `auid` Int64,
)
AS SELECT
    ts_date,
    auid,
FROM
(
    SELECT
        toDate(ts) AS ts_date,
        auid
    FROM alpha
)
WHERE ts_date <= toDateTime('2024-01-01 00:00:00') - INTERVAL 1 DAY;

INSERT INTO alpha VALUES (toDateTime64('2024-01-01 00:00:00.000', 3) - INTERVAL 3 DAY, 1);
INSERT INTO alpha VALUES (toDateTime64('2024-01-01 00:00:00.000', 3) - INTERVAL 3 DAY, 2);
INSERT INTO alpha VALUES (toDateTime64('2024-01-01 00:00:00.000', 3) - INTERVAL 3 DAY, 3);

select trimLeft(explain) from (EXPLAIN indexes = 1 SELECT auid FROM alpha__day WHERE auid = 1) where explain like '%Condition:%' or explain like '%Granules:%' SETTINGS enable_analyzer = 1;
