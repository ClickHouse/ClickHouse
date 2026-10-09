-- Regression: GROUP BY modifiers (ROLLUP / CUBE / GROUPING SETS) over Distributed -> ABR -> MergeTree.
-- These disable the memory-efficient (bucket-ordered) merge, so the initiator uses the robust
-- hash-merge path (MergingAggregatedTransform) and must NOT hit the SortingAggregatedTransform
-- "already got bucket" error. Verifies both legacy interpreter and the analyzer.

DROP TABLE IF EXISTS abr_rgs;
DROP TABLE IF EXISTS twolevel_rgs_1;
DROP TABLE IF EXISTS twolevel_rgs_10;

CREATE TABLE twolevel_rgs_1 (
    _sample_interval UInt64 DEFAULT 1,
    ts DateTime,
    key UInt64,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, key);

CREATE TABLE twolevel_rgs_10 (
    _sample_interval UInt64 DEFAULT 10,
    ts DateTime,
    key UInt64,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, key);

INSERT INTO twolevel_rgs_1
SELECT 1, toDateTime('2026-06-01 00:00:00') + INTERVAL (number % 3600) SECOND,
       number, 1
FROM numbers(1000000);

INSERT INTO twolevel_rgs_10
SELECT 10, toDateTime('2026-06-01 00:00:00') + INTERVAL (number % 3600) SECOND,
       number, 1
FROM numbers(1000000);

CREATE TABLE abr_rgs ENGINE = ABR('twolevel_rgs', [1, 10], ('ts'));

-- 1000000 distinct keys. ROLLUP/CUBE and GROUPING SETS ((key),()) add the grand-total row => 1000001.

SELECT count() FROM (
    SELECT key, sum(value) AS total
    FROM remote('localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost', currentDatabase(), 'abr_rgs')
    WHERE ts >= toDateTime('2026-06-01 00:00:00')
    GROUP BY key WITH ROLLUP
) SETTINGS distributed_aggregation_memory_efficient = 1, max_threads = 4, enable_analyzer = 0;

SELECT count() FROM (
    SELECT key, sum(value) AS total
    FROM remote('localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost', currentDatabase(), 'abr_rgs')
    WHERE ts >= toDateTime('2026-06-01 00:00:00')
    GROUP BY key WITH ROLLUP
) SETTINGS distributed_aggregation_memory_efficient = 1, max_threads = 4, enable_analyzer = 1;

SELECT count() FROM (
    SELECT key, sum(value) AS total
    FROM remote('localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost', currentDatabase(), 'abr_rgs')
    WHERE ts >= toDateTime('2026-06-01 00:00:00')
    GROUP BY key WITH CUBE
) SETTINGS distributed_aggregation_memory_efficient = 1, max_threads = 4, enable_analyzer = 0;

SELECT count() FROM (
    SELECT key, sum(value) AS total
    FROM remote('localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost', currentDatabase(), 'abr_rgs')
    WHERE ts >= toDateTime('2026-06-01 00:00:00')
    GROUP BY key WITH CUBE
) SETTINGS distributed_aggregation_memory_efficient = 1, max_threads = 4, enable_analyzer = 1;

SELECT count() FROM (
    SELECT key, sum(value) AS total
    FROM remote('localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost', currentDatabase(), 'abr_rgs')
    WHERE ts >= toDateTime('2026-06-01 00:00:00')
    GROUP BY GROUPING SETS ((key), ())
) SETTINGS distributed_aggregation_memory_efficient = 1, max_threads = 4, enable_analyzer = 0;

SELECT count() FROM (
    SELECT key, sum(value) AS total
    FROM remote('localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost,localhost', currentDatabase(), 'abr_rgs')
    WHERE ts >= toDateTime('2026-06-01 00:00:00')
    GROUP BY GROUPING SETS ((key), ())
) SETTINGS distributed_aggregation_memory_efficient = 1, max_threads = 4, enable_analyzer = 1;

DROP TABLE abr_rgs;
DROP TABLE twolevel_rgs_1;
DROP TABLE twolevel_rgs_10;

