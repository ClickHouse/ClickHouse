-- Tags: zookeeper

DROP TABLE IF EXISTS landing SYNC;
DROP TABLE IF EXISTS mv SYNC;

CREATE TABLE landing
(
    `time` DateTime,
    `number` Int64
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{database}/tables/landing/', 'r1')
PARTITION BY toYYYYMMDD(time)
ORDER BY time;

CREATE MATERIALIZED VIEW mv
ENGINE = ReplicatedSummingMergeTree('/clickhouse/{database}/tables/mv', 'r1')
PARTITION BY toYYYYMMDD(hour) ORDER BY hour
AS SELECT
       toStartOfHour(time) AS hour,
       sum(number) AS sum_amount
   FROM landing GROUP BY hour;

SELECT 'Initial';
INSERT INTO landing VALUES ('2020-01-01 13:23:34', 24);
SELECT * FROM mv ORDER BY hour;

SELECT 'Last block is duplicate';
INSERT INTO landing VALUES ('2021-09-01 11:00:00', 24), ('2020-01-01 13:23:34', 24);
SELECT * FROM mv ORDER BY hour;

SELECT 'One block is duplicate (default setting)';
SET max_insert_delayed_streams_for_parallel_write = 0;
INSERT INTO landing VALUES ('2021-09-01 11:00:00', 24), ('2022-01-01 12:03:00', 24);
SELECT * FROM mv ORDER BY hour;

SELECT 'One block is duplicate (changed setting)';
SET max_insert_delayed_streams_for_parallel_write = 5;
INSERT INTO landing VALUES ('2021-09-01 11:00:00', 24), ('2023-01-01 12:03:00', 24);

SELECT * FROM mv ORDER BY hour;

DROP TABLE mv;
DROP TABLE landing;

