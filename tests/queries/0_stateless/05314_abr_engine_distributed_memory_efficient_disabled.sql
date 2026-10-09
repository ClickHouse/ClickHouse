-- Regression: plain high-cardinality GROUP BY over Distributed -> ABR -> MergeTree,
-- with distributed_aggregation_memory_efficient toggled 0/1 and both interpreters.
--   dist_mem_eff = 1  -> initiator uses the memory-efficient zipper (SortingAggregatedTransform);
--                        requires ABR shards to emit bucket-sorted output (the isRemote() fix).
--   dist_mem_eff = 0  -> initiator uses the robust hash-merge path; must also succeed.
-- All variants return the number of distinct keys (1000000).

DROP TABLE IF EXISTS abr_me;
DROP TABLE IF EXISTS twolevel_me_1;
DROP TABLE IF EXISTS twolevel_me_10;

CREATE TABLE twolevel_me_1 (
    _sample_interval UInt64 DEFAULT 1,
    ts DateTime,
    key UInt64,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, key);

CREATE TABLE twolevel_me_10 (
    _sample_interval UInt64 DEFAULT 10,
    ts DateTime,
    key UInt64,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, key);

INSERT INTO twolevel_me_1
SELECT 1, toDateTime('2026-06-01 00:00:00') + INTERVAL (number % 3600) SECOND,
       number, 1
FROM numbers(1000000);

INSERT INTO twolevel_me_10
SELECT 10, toDateTime('2026-06-01 00:00:00') + INTERVAL (number % 3600) SECOND,
       number, 1
FROM numbers(1000000);

CREATE TABLE abr_me ENGINE = ABR('twolevel_me', [1, 10], ('ts'));

SELECT count() FROM (
    SELECT key, sum(value) AS total
    FROM remote('localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost', currentDatabase(), 'abr_me')
    WHERE ts >= toDateTime('2026-06-01 00:00:00')
    GROUP BY key
) SETTINGS distributed_aggregation_memory_efficient = 0, max_threads = 4, enable_analyzer = 0;

SELECT count() FROM (
    SELECT key, sum(value) AS total
    FROM remote('localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost', currentDatabase(), 'abr_me')
    WHERE ts >= toDateTime('2026-06-01 00:00:00')
    GROUP BY key
) SETTINGS distributed_aggregation_memory_efficient = 0, max_threads = 4, enable_analyzer = 1;

SELECT count() FROM (
    SELECT key, sum(value) AS total
    FROM remote('localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost', currentDatabase(), 'abr_me')
    WHERE ts >= toDateTime('2026-06-01 00:00:00')
    GROUP BY key
) SETTINGS distributed_aggregation_memory_efficient = 1, max_threads = 4, enable_analyzer = 0;

SELECT count() FROM (
    SELECT key, sum(value) AS total
    FROM remote('localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost', currentDatabase(), 'abr_me')
    WHERE ts >= toDateTime('2026-06-01 00:00:00')
    GROUP BY key
) SETTINGS distributed_aggregation_memory_efficient = 1, max_threads = 4, enable_analyzer = 1;

DROP TABLE abr_me;
DROP TABLE twolevel_me_1;
DROP TABLE twolevel_me_10;

