-- Tags: no-parallel-replicas

-- Constant folding must work for derived partition keys (PARTITION BY toYYYYMM(d)): within a
-- partition toYYYYMM(d) is constant, so per-partition specialization eliminates non-matching OR
-- branches and lets the primary key prune extra granules. Skip indexes / partition pruning are
-- disabled below so the EXPLAIN reflects only the primary key effect.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_derived_key;
CREATE TABLE t_derived_key (d Date, b UInt64)
ENGINE = MergeTree ORDER BY b PARTITION BY toYYYYMM(d) SETTINGS index_granularity = 1;

INSERT INTO t_derived_key VALUES ('2024-01-01', 1), ('2024-01-02', 2), ('2024-01-03', 3);
INSERT INTO t_derived_key VALUES ('2024-02-01', 4), ('2024-02-02', 5), ('2024-02-03', 6);

SELECT '== result ==';
SELECT d, b FROM t_derived_key
WHERE (toYYYYMM(d) = 202401 AND b >= 3) OR (toYYYYMM(d) = 202402 AND b > 100)
ORDER BY d, b
SETTINGS use_partition_pruning = 0, use_skip_indexes = 0, optimize_time_filter_with_preimage=0, use_constant_folding_in_index_analysis = 1;

SELECT '== granules, folding off ==';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT d, b FROM t_derived_key
    WHERE (toYYYYMM(d) = 202401 AND b >= 3) OR (toYYYYMM(d) = 202402 AND b > 10)
    SETTINGS use_partition_pruning = 0, use_skip_indexes = 0, optimize_time_filter_with_preimage=0, use_constant_folding_in_index_analysis = 0
) WHERE explain LIKE '%Granules:%';

SELECT '== granules, folding on ==';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT d, b FROM t_derived_key
    WHERE (toYYYYMM(d) = 202401 AND b >= 3) OR (toYYYYMM(d) = 202402 AND b > 10)
    SETTINGS use_partition_pruning = 0, use_skip_indexes = 0, optimize_time_filter_with_preimage=0, use_constant_folding_in_index_analysis = 1
) WHERE explain LIKE '%Granules:%';

DROP TABLE t_derived_key;
