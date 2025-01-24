SET session_timezone = 'Etc/UTC';

DROP TABLE IF EXISTS tt;
CREATE TABLE tt
(
    `id` Int64,
    `ts` DateTime
)
ENGINE = MergeTree()
ORDER BY dateTrunc('hour', ts)
SETTINGS index_granularity = 8192;

INSERT INTO tt VALUES (1, '2024-11-14 00:00:00'), (2, '2024-11-14 00:00:00');

SELECT id FROM tt PREWHERE ts BETWEEN toDateTime(1731506400) AND toDateTime(1731594420);

explain indexes=1, description=0 SELECT id FROM tt PREWHERE ts BETWEEN toDateTime(1731506400) AND toDateTime(1731594420);

DROP TABLE tt;
