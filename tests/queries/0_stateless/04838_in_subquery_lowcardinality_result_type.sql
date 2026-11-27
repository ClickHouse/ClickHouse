-- Tags: no-old-analyzer
-- no-old-analyzer: the distributed-plan probe requires the analyzer.

-- The result type of `IN` over a `LowCardinality` argument must not depend on whether the
-- right-hand side is a literal list or a subquery: the set is a constant column in the plan
-- in both cases, so both forms get the `LowCardinality`-wrapped result type. The subquery
-- form used to get plain `UInt8`; a worker deserializing a distributed plan rebuilt the
-- function with the wrapped type and failed the plan type check.

DROP TABLE IF EXISTS t_lc_in;
CREATE TABLE t_lc_in (s LowCardinality(String), k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_lc_in SELECT toString(number % 4), number FROM numbers(100);

SELECT '-- the literal and the subquery forms have the same type';
SELECT DISTINCT toTypeName(s IN ('0', '1')) FROM t_lc_in;
SELECT DISTINCT toTypeName(s IN (SELECT toString(number) FROM numbers(2))) FROM t_lc_in;

SELECT '-- results agree';
SELECT count() FROM t_lc_in WHERE s IN ('0', '1');
SELECT count() FROM t_lc_in WHERE s IN (SELECT toString(number) FROM numbers(2));

SELECT '-- the distributed plan round-trip accepts the type';
SELECT count() FROM t_lc_in WHERE s IN (SELECT toString(number) FROM numbers(2))
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
        enable_parallel_replicas = 0, max_rows_to_group_by = 0;

DROP TABLE t_lc_in;
