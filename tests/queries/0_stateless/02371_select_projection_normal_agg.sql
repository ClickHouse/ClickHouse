DROP TABLE IF EXISTS video_log;

CREATE TABLE video_log
(
    `datetime` DateTime,
    `user_id` UInt64,
    `device_id` UInt64,
    `domain` LowCardinality(String),
    `bytes` UInt64,
    `duration` UInt64
)
ENGINE = MergeTree
PARTITION BY toDate(datetime)
ORDER BY (user_id, device_id);

DROP TABLE IF EXISTS rng;

CREATE TABLE rng
(
    `user_id_raw` UInt64,
    `device_id_raw` UInt64,
    `domain_raw` UInt64,
    `bytes_raw` UInt64,
    `duration_raw` UInt64
)
ENGINE = GenerateRandom(1024);

INSERT INTO video_log SELECT
  toUnixTimestamp('2022-07-22 01:00:00')
  + (rowNumberInAllBlocks() / 20000),
  user_id_raw % 100000000 AS user_id,
  device_id_raw % 200000000 AS device_id,
  domain_raw % 100,
  (bytes_raw % 1024) + 128,
  (duration_raw % 300) + 100
FROM rng
LIMIT 1728000;

INSERT INTO video_log SELECT
  toUnixTimestamp('2022-07-22 01:00:00')
  + (rowNumberInAllBlocks() / 20000),
  user_id_raw % 100000000 AS user_id,
  100 AS device_id,
  domain_raw % 100,
  (bytes_raw % 1024) + 128,
  (duration_raw % 300) + 100
FROM rng
LIMIT 10;

DROP TABLE IF EXISTS video_log_result;

CREATE TABLE video_log_result
(
    `hour` DateTime,
    `sum_bytes` UInt64,
    `avg_duration` Float64
)
ENGINE = MergeTree
PARTITION BY toDate(hour)
ORDER BY sum_bytes;

INSERT INTO video_log_result SELECT
    toStartOfHour(datetime) AS hour,
    sum(bytes),
    avg(duration)
FROM video_log
WHERE (toDate(hour) = '2022-07-22') AND (device_id = '100') --(device_id = '100') Make sure it's not good and doesn't go into prewhere.
GROUP BY hour;


ALTER TABLE video_log ADD PROJECTION p_norm
(
    SELECT
        datetime,
        device_id,
        bytes,
        duration
    ORDER BY device_id
);

ALTER TABLE video_log MATERIALIZE PROJECTION p_norm settings mutations_sync=1;

ALTER TABLE video_log ADD PROJECTION p_agg
(
    SELECT
        toStartOfHour(datetime) AS hour,
        domain,
        sum(bytes),
        avg(duration)
    GROUP BY
        hour,
        domain
);

ALTER TABLE video_log MATERIALIZE PROJECTION p_agg settings mutations_sync=1;

SELECT
    equals(sum_bytes1, sum_bytes2),
    equals(avg_duration1, avg_duration2)
FROM
(
    SELECT
        toStartOfHour(datetime) AS hour,
        sum(bytes) AS sum_bytes1,
        avg(duration) AS avg_duration1
    FROM video_log
    WHERE (toDate(hour) = '2022-07-22') AND (device_id = '100') --(device_id = '100') Make sure it's not good and doesn't go into prewhere.
    GROUP BY hour
)
LEFT JOIN
(
    SELECT
        `hour`,
        `sum_bytes` AS sum_bytes2,
        `avg_duration` AS avg_duration2
    FROM video_log_result
)
USING (hour) settings joined_subquery_requires_alias=0;

DROP TABLE IF EXISTS video_log;

DROP TABLE IF EXISTS rng;

DROP TABLE IF EXISTS video_log_result;
