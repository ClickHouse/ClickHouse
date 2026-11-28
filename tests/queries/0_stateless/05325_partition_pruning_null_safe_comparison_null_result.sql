-- Tags: no-parallel-replicas, no-replicated-database
-- no-parallel-replicas, no-replicated-database: the EXPLAIN block prints the local plan.
-- https://github.com/ClickHouse/ClickHouse/actions/runs/37295117105/job/111742340948

SET explain_query_plan_default = 'legacy';
SET parallel_replicas_local_plan = 1;
SET use_statistics = 0, use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_part;
DROP TABLE IF EXISTS t_flat;
DROP TABLE IF EXISTS t_square;

-- `t_flat` has the same rows and no partition key, so its answer is the row-level truth.
CREATE TABLE t_part (x Int32) ENGINE = MergeTree PARTITION BY x ORDER BY tuple();
CREATE TABLE t_flat (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_part SELECT number FROM numbers(15);
INSERT INTO t_flat SELECT number FROM numbers(15);

-- `moduloOrNull(7, x)` and `nullIf(x, 0)` are NULL for x = 0, and `NULL <=> c` is false, so its negation keeps that row.
SELECT 'NOT (-5 <=> moduloOrNull(7, x))', (SELECT count() FROM t_flat WHERE NOT (-5 <=> moduloOrNull(7, x))), (SELECT count() FROM t_part WHERE NOT (-5 <=> moduloOrNull(7, x)));
SELECT 'NOT (moduloOrNull(7, x) IS NOT DISTINCT FROM 0)', (SELECT count() FROM t_flat WHERE NOT (moduloOrNull(7, x) IS NOT DISTINCT FROM 0)), (SELECT count() FROM t_part WHERE NOT (moduloOrNull(7, x) IS NOT DISTINCT FROM 0));
SELECT 'NOT (nullIf(x, 0) <=> 5)', (SELECT count() FROM t_flat WHERE NOT (nullIf(x, 0) <=> 5)), (SELECT count() FROM t_part WHERE NOT (nullIf(x, 0) <=> 5));
SELECT 'NOT ((moduloOrNull(7, x) = -5) IS TRUE)', (SELECT count() FROM t_flat WHERE NOT ((moduloOrNull(7, x) = -5) IS TRUE)), (SELECT count() FROM t_part WHERE NOT ((moduloOrNull(7, x) = -5) IS TRUE));

-- Only the partition whose value maps to NULL loses pruning: x = 1 and x = 7 (7 % x = 0) are still pruned.
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1 SELECT x FROM t_part WHERE NOT (moduloOrNull(7, x) <=> 0)
) WHERE explain ILIKE '%Partition%' OR explain ILIKE '%Parts:%';

-- The fuzzer's table and predicate: count(p) + count(NOT p) + count(p IS NULL) = count().
CREATE TABLE t_square (x Int32) ENGINE = MergeTree PARTITION BY x * x ORDER BY tuple();
INSERT INTO t_square SELECT number FROM numbers(15);
SELECT (SELECT count() FROM t_square WHERE -2147483647 <=> positiveModuloOrNull(7, x * x))
     + (SELECT count() FROM t_square WHERE NOT (-2147483647 <=> positiveModuloOrNull(7, x * x)))
     + (SELECT count() FROM t_square WHERE (-2147483647 <=> positiveModuloOrNull(7, x * x)) IS NULL)
     = (SELECT count() FROM t_square) AS tlp_identity_holds;

DROP TABLE t_part;
DROP TABLE t_flat;
DROP TABLE t_square;
