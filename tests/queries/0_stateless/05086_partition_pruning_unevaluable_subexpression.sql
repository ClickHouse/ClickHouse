-- Tags: no-parallel-replicas
-- https://github.com/ClickHouse/ClickHouse/issues/115271
-- Index analysis evaluates a monotonic function chain on the endpoints of a key range, which are
-- values the analysis substitutes rather than values the query asked about. `intDiv(1, p - 1)` divides
-- by zero on the boundary `p = 1` even under `WHERE p != 1`, and the error escaped: the query threw
-- `Division by zero` with `use_skip_indexes = 0`, while the default settings returned the correct 0
-- only because the minmax index happened to prune the part before the partition pruner was asked.
-- This is the `use_skip_indexes = 0` face of the contract 04312 states.

SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_pruning_eval_error;
CREATE TABLE t_pruning_eval_error (p Int64, b UInt64) ENGINE = MergeTree ORDER BY b PARTITION BY p
SETTINGS index_granularity = 1;
INSERT INTO t_pruning_eval_error VALUES (1, 10);

SELECT count() FROM t_pruning_eval_error WHERE p != 1 AND intDiv(1, p - 1) > 0;
SELECT count() FROM t_pruning_eval_error WHERE p != 1 AND intDiv(1, p - 1) > 0 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_pruning_eval_error WHERE p != 1 AND intDiv(1, p - 1) > 0 SETTINGS use_skip_indexes = 0, use_primary_key = 0;
SELECT count() FROM t_pruning_eval_error WHERE p != 1 AND intDiv(1, p - 1) > 0 SETTINGS use_skip_indexes = 0, use_partition_pruning = 0;
SELECT count() FROM t_pruning_eval_error WHERE p != 1 AND intDiv(1, p - 1) > 0 SETTINGS use_constant_folding_in_index_analysis = 1;
SELECT count() FROM t_pruning_eval_error WHERE p != 1 AND intDiv(1, p - 1) > 0 SETTINGS optimize_use_implicit_projections = 0;

SELECT 'primary key';
DROP TABLE IF EXISTS t_pruning_eval_error_pk;
CREATE TABLE t_pruning_eval_error_pk (k Int64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_pruning_eval_error_pk VALUES (1), (5);
SELECT count() FROM t_pruning_eval_error_pk WHERE k != 1 AND intDiv(1, k - 1) > 0;
SELECT count() FROM t_pruning_eval_error_pk WHERE k != 1 AND intDiv(1, k - 1) > 0 SETTINGS use_skip_indexes = 0;
SELECT countIf(k != 1 AND intDiv(1, if(k = 1, 2, k - 1)) > 0) FROM t_pruning_eval_error_pk;

SELECT 'pruning still works';
DROP TABLE IF EXISTS t_pruning_still_works;
CREATE TABLE t_pruning_still_works (p Int64, b UInt64) ENGINE = MergeTree ORDER BY b PARTITION BY p;
INSERT INTO t_pruning_still_works VALUES (1, 10), (2, 20), (3, 30);
SELECT count() FROM t_pruning_still_works WHERE p = 2;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT b FROM t_pruning_still_works WHERE p = 2) WHERE explain LIKE '%Parts: 1/3%';

DROP TABLE t_pruning_eval_error;
DROP TABLE t_pruning_eval_error_pk;
DROP TABLE t_pruning_still_works;
