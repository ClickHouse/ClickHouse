DROP TABLE IF EXISTS ttl_group_by_bug;

CREATE TABLE ttl_group_by_bug
(key UInt32, ts DateTime, value UInt32, min_value UInt32 default value, max_value UInt32 default value)
ENGINE = MergeTree() PARTITION BY toYYYYMM(ts)
ORDER BY (key, toStartOfInterval(ts, toIntervalMinute(3)), ts)
TTL ts + INTERVAL 5 MINUTE GROUP BY key, toStartOfInterval(ts, toIntervalMinute(3))
SET value = sum(value), min_value = min(min_value), max_value = max(max_value),  ts=min(toStartOfInterval(ts, toIntervalMinute(3)));

INSERT INTO ttl_group_by_bug(key, ts, value) SELECT number%5 as key, now() - interval 10 minute + number, 0 FROM numbers(1000);

OPTIMIZE TABLE ttl_group_by_bug FINAL;

SELECT *
FROM
(
    SELECT
        _part,
        rowNumberInAllBlocks(),
        (key, toStartOfInterval(ts, toIntervalMinute(3)), ts) AS cur,
        lagInFrame((key, toStartOfInterval(ts, toIntervalMinute(3)), ts), 1) OVER () AS prev,
        1
    FROM ttl_group_by_bug
)
WHERE cur < prev
LIMIT 2
SETTINGS max_threads = 1;

DROP TABLE IF EXISTS ttl_group_by_bug;
