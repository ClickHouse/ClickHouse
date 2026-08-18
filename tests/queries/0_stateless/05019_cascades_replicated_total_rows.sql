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
SET automatic_parallel_replicas_mode = 0;

DROP TABLE IF EXISTS t_cascades_repl SYNC;
CREATE TABLE t_cascades_repl (k UInt64, x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_cascades_repl', 'r1')
    ORDER BY k;
INSERT INTO t_cascades_repl SELECT number % 5, number FROM numbers(1000);

SELECT count() FROM t_cascades_repl;
SELECT k, sum(x) FROM t_cascades_repl GROUP BY k ORDER BY k;
SELECT count() FROM t_cascades_repl AS a JOIN t_cascades_repl AS b USING (k);

-- A filter on a non-key column leaves the row count unestimated, so the group's statistics are
-- derived on demand instead of being prepopulated. `use_statistics = 0` keeps the column-statistics
-- estimator from supplying a count and reaching the prepopulated path anyway.
SELECT k, count() FROM t_cascades_repl WHERE x > 10 GROUP BY k ORDER BY k SETTINGS use_statistics = 0;

DROP TABLE t_cascades_repl SYNC;
