-- Tags: no-random-settings, no-random-merge-tree-settings

-- Regression for #108129: the deferred (exact-size) parallel_hash build computes the scatter hashes of
-- the rows it will actually insert in `selectDispatchBlock`, which calls `extractNestedColumnsAndNullMap`
-- to fold the nullable key columns into one combined null map. When two or more key columns are nullable
-- that call allocates a fresh OR-combined null map and returns the owning column; the deferred path used
-- to discard that holder, leaving the `null_map` pointer dangling and reading freed memory while filtering
-- the insertable rows -- an exception/segfault on the build thread (seen on the spilling parallel_hash
-- path under ASan/coverage). The build below has two nullable keys with some NULLs so the combined null
-- map is taken, and a duplicated/filtered right side so the deferred row-filtering loop runs.

SET collect_hash_table_stats_during_joins = 0; -- no size hint => the deferred build path
SET parallel_hash_join_threshold = 0; -- force ConcurrentHashJoin regardless of the build size
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 'false';
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_probe;

-- Two nullable equality keys on the build side; ~1/7 of k1 and ~1/11 of k2 are NULL so
-- `extractNestedColumnsAndNullMap` builds a real combined null map. Keys repeat so the deferred build
-- buffers and the insertable-rows filter has work to do.
CREATE TABLE t_build (k1 Nullable(UInt64), k2 Nullable(UInt64), v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe (k1 Nullable(UInt64), k2 Nullable(UInt64), v UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_build
SELECT
    if(number % 7 = 0, NULL, number % 5000) AS k1,
    if(number % 11 = 0, NULL, number % 4000) AS k2,
    number AS v
FROM numbers(200000);

INSERT INTO t_probe
SELECT
    if(number % 13 = 0, NULL, number % 5000) AS k1,
    if(number % 17 = 0, NULL, number % 4000) AS k2,
    number AS v
FROM numbers(50000);

-- Reference: plain hash join.
SET join_algorithm = 'hash';
SELECT 'multi nullable', count(), sum(cityHash64(l.v, r.v))
FROM t_probe l INNER JOIN t_build r ON l.k1 = r.k1 AND l.k2 = r.k2;

-- The deferred parallel_hash build must give the same result and not read the freed combined null map.
SET join_algorithm = 'parallel_hash';
SELECT 'multi nullable', count(), sum(cityHash64(l.v, r.v))
FROM t_probe l INNER JOIN t_build r ON l.k1 = r.k1 AND l.k2 = r.k2;

DROP TABLE t_build;
DROP TABLE t_probe;
