DROP DATABASE IF EXISTS test_apply_ttl_on_fly_db;

CREATE DATABASE IF NOT EXISTS test_apply_ttl_on_fly_db;

CREATE TABLE IF NOT EXISTS test_apply_ttl_on_fly_db.test_apply_ttl_on_fly
(
    event_time DateTime,
    user_id UInt64,
    event_type String,
    value UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, event_time)
TTL event_time + INTERVAL 1 SECOND   -- TTL expires 1 second after event_time
SETTINGS index_granularity = 8192;

-- Disable background merges to prevent automatic TTL cleanup
SYSTEM STOP MERGES test_apply_ttl_on_fly_db.test_apply_ttl_on_fly;

-- Insert test data
INSERT INTO test_apply_ttl_on_fly_db.test_apply_ttl_on_fly
SELECT
    now() AS event_time,
    number AS user_id,
    arrayElement(['click', 'view', 'error'], number % 3 + 1) AS event_type,
    1 AS value
FROM numbers(10);

-- Data should exist before TTL is applied
SELECT count() AS cnt_before
FROM test_apply_ttl_on_fly_db.test_apply_ttl_on_fly;

-- Wait until TTL condition becomes true
SELECT sleep(2);

-- Query with apply_ttl_on_fly enabled (expired rows are filtered)
SELECT *
FROM test_apply_ttl_on_fly_db.test_apply_ttl_on_fly
SETTINGS apply_ttl_on_fly = 1;

DROP DATABASE IF EXISTS test_apply_ttl_on_fly_db;
