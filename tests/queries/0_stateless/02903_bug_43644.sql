DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    `machine_id` UInt64,
    `name` String,
    `timestamp` DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY machine_id;

insert into tab(machine_id, name, timestamp)
select 1, 'a_name', '2022-11-24 12:00:00';

SELECT
  toStartOfInterval(timestamp, INTERVAL 300 SECOND) AS ts
FROM tab
WHERE ts > '2022-11-24 11:19:00'
GROUP BY ts;

DROP TABLE tab;
