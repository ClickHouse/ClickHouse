-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings
-- EXPLAIN output may differ

-- { echo }

DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    d Date,
    x UInt8
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d);

INSERT INTO test VALUES
    ('2026-01-01', 1),
    ('2026-02-01', 2);

SELECT *
FROM test
WHERE d = '2026-01-01'
SETTINGS use_partition_pruning = 1;

EXPLAIN indexes = 1
SELECT *
FROM test
WHERE d = '2026-01-01'
SETTINGS use_partition_pruning = 1;

SELECT *
FROM test
WHERE d = '2026-01-01'
SETTINGS use_partition_pruning = 0;

EXPLAIN indexes = 1
SELECT *
FROM test
WHERE d = '2026-01-01'
SETTINGS use_partition_pruning = 0;


DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    p DateTime,
    k Int32
)
ENGINE = MergeTree
PARTITION BY toDate(p)
SETTINGS index_granularity = 1;

INSERT INTO test VALUES
    ('2020-09-01 00:01:02', 1),
    ('2020-09-01 20:01:03', 2),
    ('2021-09-02 00:01:03', 3);

SELECT count()
FROM test
WHERE toYear(toDate(p)) = 2020
SETTINGS
    enable_analyzer = 0,
    optimize_use_implicit_projections = 0,
    use_partition_pruning = 1;

EXPLAIN indexes = 1
SELECT count()
FROM test
WHERE toYear(toDate(p)) = 2020
SETTINGS
    enable_analyzer = 0,
    optimize_use_implicit_projections = 0,
    use_partition_pruning = 1;

SELECT count()
FROM test
WHERE toYear(toDate(p)) = 2020
SETTINGS
    enable_analyzer = 0,
    optimize_use_implicit_projections = 0,
    use_partition_pruning = 0;

EXPLAIN indexes = 1
SELECT count()
FROM test
WHERE toYear(toDate(p)) = 2020
SETTINGS
    enable_analyzer = 0,
    optimize_use_implicit_projections = 0,
    use_partition_pruning = 0;


-- `use_partition_key` is an alias
EXPLAIN indexes = 1
SELECT count()
FROM test
WHERE toYear(toDate(p)) = 2020
SETTINGS
    enable_analyzer = 0,
    optimize_use_implicit_projections = 0,
    use_partition_key = 1;

EXPLAIN indexes = 1
SELECT count()
FROM test
WHERE toYear(toDate(p)) = 2020
SETTINGS
    enable_analyzer = 0,
    optimize_use_implicit_projections = 0,
    use_partition_key = 0;
