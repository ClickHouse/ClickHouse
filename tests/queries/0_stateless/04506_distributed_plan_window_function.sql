-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Window functions can be executed under make_distributed_plan=1: WindowStep is serialized for remote
-- execution and produces the same result as the non-distributed plan. Only windows whose feeding sort
-- is serializable (no PARTITION BY) can be distributed for now; a PARTITION BY window is rejected (see
-- the fail-close assertion at the end) because its partitioned sort cannot preserve per-partition order
-- across the exchange yet.

DROP TABLE IF EXISTS t_window_dist;

CREATE TABLE t_window_dist (a UInt32, v UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_window_dist SELECT number % 5, number FROM numbers(20);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0;

SELECT '-- sum OVER (ORDER BY)';
SELECT v, sum(v) OVER (ORDER BY v) AS s FROM t_window_dist ORDER BY v;

SELECT '-- row_number OVER (ORDER BY)';
SELECT v, row_number() OVER (ORDER BY v) AS rn FROM t_window_dist ORDER BY v;

SELECT '-- rolling frame (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)';
SELECT v, sum(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS roll FROM t_window_dist ORDER BY v;

SELECT '-- PARTITION BY window is not serializable for remote execution yet -> fail closed';
SELECT a, v, sum(v) OVER (PARTITION BY a ORDER BY v) FROM t_window_dist ORDER BY a, v; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_window_dist;

-- Sanity check that deserializing a WindowStep picks the `window_creator` state variant for
-- CrossTab-family aggregates (cramersV, cramersVBiasCorrected, contingency, theilsU) rather than
-- falling back to the ordinary aggregation state variant. The non-window variant re-scans the whole
-- frame on every row, so it would not finish in reasonable time for this input size. The data comes
-- from a MergeTree table because ReadFromSystemNumbers is not serializable for remote execution.
DROP TABLE IF EXISTS t_window_crosstab;
CREATE TABLE t_window_crosstab (n UInt32) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_window_crosstab SELECT number FROM numbers(100000);

SELECT '-- theilsU window variant under make_distributed_plan';
SELECT n, round(tu, 2)
FROM
(
    SELECT n, theilsU(n, n) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND 99999 FOLLOWING) AS tu
    FROM t_window_crosstab
)
ORDER BY n
LIMIT 1;

DROP TABLE t_window_crosstab;
