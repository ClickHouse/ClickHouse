CREATE TABLE test_ttl_group_by_summing_01763
(
    `key1` UInt32,
    `key2` UInt32,
    `ts` DateTime,
    `value` UInt32,
    `min_value` SimpleAggregateFunction(min, UInt32) 
                                       DEFAULT value,
    `max_value` SimpleAggregateFunction(max, UInt32) 
                                       DEFAULT value
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(ts)
PRIMARY KEY (key1, key2, toStartOfDay(ts))
ORDER BY (key1, key2, toStartOfDay(ts), ts)
TTL ts + interval 30 day 
    GROUP BY key1, key2, toStartOfDay(ts) 
    SET value = sum(value), 
    min_value = min(min_value), 
    max_value = max(max_value), 
    ts = min(toStartOfDay(ts));


SYSTEM STOP TTL MERGES test_ttl_group_by_summing_01763;
SYSTEM STOP MERGES test_ttl_group_by_summing_01763;

INSERT INTO test_ttl_group_by_summing_01763 (key1, key2, ts, value)
SELECT
    1,
    2,
    toStartOfMinute(now() + number*60),
    1
FROM numbers(100);

INSERT INTO test_ttl_group_by_summing_01763 (key1, key2, ts, value)
SELECT
    1,
    2,
    toStartOfMinute(now() + number*60),
    1
FROM numbers(100);

INSERT INTO test_ttl_group_by_summing_01763 (key1, key2, ts, value)
SELECT
    1,
    2,
    toStartOfMinute(now() + number*60 - toIntervalDay(60)),
    2
FROM numbers(100);

INSERT INTO test_ttl_group_by_summing_01763 (key1, key2, ts, value)
SELECT
    1,
    2,
    toStartOfMinute(now() + number*60 - toIntervalDay(60)),
    2
FROM numbers(100);

SELECT
    toYYYYMM(ts) AS m,
    count(),
    sum(value),
    min(min_value),
    max(max_value)
FROM test_ttl_group_by_summing_01763
GROUP BY m;


SYSTEM START TTL MERGES test_ttl_group_by_summing_01763;
SYSTEM START MERGES test_ttl_group_by_summing_01763;
OPTIMIZE TABLE test_ttl_group_by_summing_01763 FINAL;

SELECT
    toYYYYMM(ts) AS m,
    count(),
    sum(value),
    min(min_value),
    max(max_value)
FROM test_ttl_group_by_summing_01763
GROUP BY m;
