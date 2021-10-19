DROP TABLE IF EXISTS test_ttl_group_by01763;
CREATE TABLE test_ttl_group_by01763
(key UInt32, ts DateTime, value UInt32, min_value UInt32 default value, max_value UInt32 default value)
ENGINE = MergeTree() PARTITION BY toYYYYMM(ts)
ORDER BY (key, toStartOfInterval(ts, toIntervalMinute(3)), ts) 
TTL ts + INTERVAL 5 MINUTE GROUP BY key, toStartOfInterval(ts, toIntervalMinute(3)) 
SET value = sum(value), min_value = min(min_value), max_value = max(max_value),  ts=min(toStartOfInterval(ts, toIntervalMinute(3)));

INSERT INTO test_ttl_group_by01763(key, ts, value) SELECT number%5 as key, now() - interval 10 minute + number, 1 FROM numbers(100000);
INSERT INTO test_ttl_group_by01763(key, ts, value) SELECT number%5 as key, now() - interval 10 minute + number, 0 FROM numbers(1000);
INSERT INTO test_ttl_group_by01763(key, ts, value) SELECT number%5 as key, now() - interval 10 minute + number, 3 FROM numbers(1000);
INSERT INTO test_ttl_group_by01763(key, ts, value) SELECT number%5 as key, now() -   interval 2 month + number, 1 FROM numbers(100000);
INSERT INTO test_ttl_group_by01763(key, ts, value) SELECT number%5 as key, now() -   interval 2 month + number, 0 FROM numbers(1000);
INSERT INTO test_ttl_group_by01763(key, ts, value) SELECT number%5 as key, now() -   interval 2 month + number, 3 FROM numbers(1000);

SELECT sum(value), min(min_value), max(max_value), uniqExact(key) FROM test_ttl_group_by01763;
SELECT sum(value), min(min_value), max(max_value), uniqExact(key) FROM test_ttl_group_by01763 where key = 3 ;
SELECT sum(value), min(min_value), max(max_value), uniqExact(key) FROM test_ttl_group_by01763 where key = 3 and ts <= today() - interval 30 day ;

OPTIMIZE TABLE test_ttl_group_by01763 FINAL;

SELECT sum(value), min(min_value), max(max_value), uniqExact(key) FROM test_ttl_group_by01763;
SELECT sum(value), min(min_value), max(max_value), uniqExact(key) FROM test_ttl_group_by01763 where key = 3 ;
SELECT sum(value), min(min_value), max(max_value), uniqExact(key) FROM test_ttl_group_by01763 where key = 3 and ts <= today() - interval 30 day ;

DROP TABLE test_ttl_group_by01763;
