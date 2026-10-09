-- Tags: no-random-settings
-- ^ EXPLAIN PIPELINE of a Distributed query is sensitive to prefer_localhost_replica
--   Disable random settings and pin the value so the plan is deterministic in CI

DROP TABLE IF EXISTS abr_2l;
DROP TABLE IF EXISTS twolevel_1;
DROP TABLE IF EXISTS twolevel_10;

CREATE TABLE twolevel_1 (
    _sample_interval UInt64 DEFAULT 1,
    ts DateTime,
    key UInt64,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, key);

CREATE TABLE twolevel_10 (
    _sample_interval UInt64 DEFAULT 10,
    ts DateTime,
    key UInt64,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, key);

INSERT INTO twolevel_1
SELECT 1, toDateTime('2026-06-01 00:00:00') + INTERVAL (number % 3600) SECOND,
       number, 1
FROM numbers(1000000);

INSERT INTO twolevel_10
SELECT 10, toDateTime('2026-06-01 00:00:00') + INTERVAL (number % 3600) SECOND,
       number, 1
FROM numbers(1000000);

CREATE TABLE abr_2l ENGINE = ABR('twolevel', [1, 10], ('ts'));

EXPLAIN PIPELINE
SELECT key, sum(value) AS total
FROM remote('localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost', currentDatabase(), 'abr_2l')
WHERE ts >= toDateTime('2026-06-01 00:00:00')
GROUP BY key
SETTINGS distributed_aggregation_memory_efficient = 1, max_threads = 4,
         optimize_aggregation_in_order = 0, async_socket_for_remote = 1,
         prefer_localhost_replica = 1, enable_analyzer = 0;

-- Execute the query — triggers the duplicate-bucket error before the fix.
-- Also assert correctness: 1000000 distinct keys; each key is summed across the
-- 30 identical localhost shards (value=1 => 30 per key), so sum(total)=30000000.
-- A silently-wrong merge (dropped/duplicated buckets) would change these numbers.
SELECT count(), sum(total)
FROM (
    SELECT key, sum(value) AS total
    FROM remote('localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost', currentDatabase(), 'abr_2l')
    WHERE ts >= toDateTime('2026-06-01 00:00:00')
    GROUP BY key
)
SETTINGS distributed_aggregation_memory_efficient = 1, max_threads = 4,
         optimize_aggregation_in_order = 0, async_socket_for_remote = 1,
         prefer_localhost_replica = 1, enable_analyzer = 0;

DROP TABLE abr_2l;
DROP TABLE twolevel_1;
DROP TABLE twolevel_10;

