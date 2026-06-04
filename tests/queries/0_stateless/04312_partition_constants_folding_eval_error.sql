-- Tags: no-parallel-replicas

-- Constant folding in index analysis must never throw when a substituted partition constant makes a
-- sub-expression fail to evaluate (e.g. intDiv(1, p - 1) with p = 1 -> division by zero). The result
-- must match the unfolded path, and analysis must not throw even when all index analysis is disabled.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_folding_eval_error;

CREATE TABLE t_folding_eval_error (p Int64, b UInt64)
ENGINE = MergeTree
ORDER BY b
PARTITION BY p
SETTINGS index_granularity = 1;

INSERT INTO t_folding_eval_error VALUES (1, 10);

SELECT count() FROM t_folding_eval_error WHERE p != 1 AND intDiv(1, p - 1) > 0
SETTINGS use_constant_folding_in_index_analysis = 0;

SELECT count() FROM t_folding_eval_error WHERE p != 1 AND intDiv(1, p - 1) > 0
SETTINGS use_constant_folding_in_index_analysis = 1;

SELECT count() FROM t_folding_eval_error WHERE p != 1 AND intDiv(1, p - 1) > 0
SETTINGS
    use_primary_key = 0,
    use_partition_pruning = 0,
    use_skip_indexes = 0,
    use_constant_folding_in_index_analysis = 1;

DROP TABLE t_folding_eval_error;
