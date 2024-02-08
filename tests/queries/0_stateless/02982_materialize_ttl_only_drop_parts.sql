-- { echoOn }
DROP TABLE IF EXISTS test_ttl;

CREATE TABLE test_ttl
(
    `ts` DateTime CODEC(Delta, LZ4),
    `huge_column1` String CODEC(NONE),
    `huge_column2` String CODEC(NONE),
    `huge_column3` String CODEC(NONE)
)
    ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY tuple() AS
SELECT
    now() - INTERVAL 1440 DAY + toIntervalSecond(number * 5) AS ts,
    randomPrintableASCII(100) AS huge_column1,
    randomPrintableASCII(120) AS huge_column2,
    randomPrintableASCII(130) AS huge_column3
FROM numbers(100000);

SELECT
    _part,
    min(ts),
    max(ts),
    count()
FROM test_ttl
GROUP BY _part ORDER BY max(ts) desc;

SET mutations_sync = 1;

ALTER TABLE test_ttl
    MODIFY SETTING ttl_only_drop_parts = 1;

ALTER TABLE test_ttl
    MODIFY TTL ts + toIntervalDay(1439);

SELECT
    _part,
    min(ts),
    max(ts),
    count()
FROM test_ttl
GROUP BY _part ORDER BY max(ts) desc;

ALTER TABLE test_ttl
    MODIFY TTL ts + toIntervalDay(1437);

SELECT
    _part,
    min(ts),
    max(ts),
    count()
FROM test_ttl
GROUP BY _part ORDER BY max(ts) desc;
