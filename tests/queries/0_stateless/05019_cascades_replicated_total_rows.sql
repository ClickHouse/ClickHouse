-- Tags: no-darwin, no-old-analyzer, zookeeper
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer, like the other make_distributed_plan tests.
-- zookeeper: the estimated table is ReplicatedMergeTree.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET distributed_plan_workers_num = 4;
SET enable_parallel_replicas = 0;
-- Distributed aggregation cannot enforce a global `max_rows_to_group_by`, so the functional-test
-- profile's non-zero limit would leave a single-node local aggregation and no distributed read.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_cascades_repl SYNC;
CREATE TABLE t_cascades_repl (k UInt64, x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_cascades_repl', 'r1')
    ORDER BY k;
INSERT INTO t_cascades_repl SELECT number % 5, number FROM numbers(1000);

SELECT count() FROM t_cascades_repl;
SELECT k, sum(x) FROM t_cascades_repl GROUP BY k ORDER BY k;
SELECT count() FROM t_cascades_repl AS a JOIN t_cascades_repl AS b USING (k);

-- A filter on a non-key column leaves the row count unestimated, so the group's statistics are
-- derived on demand instead of being prepopulated. `PREWHERE` puts the filter on the read step
-- itself, which is what makes the count unestimable; `use_statistics = 0` keeps the
-- column-statistics estimator from supplying one.
SELECT k, count() FROM t_cascades_repl PREWHERE x > 10 GROUP BY k ORDER BY k SETTINGS use_statistics = 0;

-- The results above are only meaningful if the distributed optimizer planned those queries, and a
-- distributed read strategy is named in the plan only when it did. The table is small, so drop the
-- fixed exchange cost, which would otherwise make the single-node read the cheapest plan. The outer
-- query runs without the optimizer, which refuses to read an `EXPLAIN`.
SET param__internal_cascades_cost_config = '{"exchange_fixed_overhead":1}';

SELECT countIf(explain LIKE '%ParallelRead%' OR explain LIKE '%ReplicatedRead%') > 0
FROM (
    EXPLAIN
    SELECT count() FROM t_cascades_repl
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1
)
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

SELECT countIf(explain LIKE '%ParallelRead%' OR explain LIKE '%ReplicatedRead%') > 0
FROM (
    EXPLAIN
    SELECT k, count() FROM t_cascades_repl PREWHERE x > 10 GROUP BY k ORDER BY k
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, use_statistics = 0
)
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE t_cascades_repl SYNC;
