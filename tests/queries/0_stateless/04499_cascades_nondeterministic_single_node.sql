-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer, like the other make_distributed_plan tests.

-- A step with a per-block or non-deterministic function (`rowNumberInAllBlocks` here) must run
-- on a single node: split across N nodes each one counts its own stream from zero, so a filter
-- `rowNumberInAllBlocks() < 1000` returns up to N * 1000 rows instead of 1000.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_nondet;

CREATE TABLE t_nondet (k UInt64, v UInt64) ENGINE = MergeTree() ORDER BY k;
INSERT INTO t_nondet SELECT number, number * 2 FROM numbers(100000);

SELECT '-- rowNumberInAllBlocks filter runs on a single node';
SELECT count() FROM (SELECT * FROM t_nondet WHERE rowNumberInAllBlocks() < 1000)
SETTINGS distributed_plan_execute_locally = 1;

SELECT '-- Baseline without Cascades';
SELECT count() FROM (SELECT * FROM t_nondet WHERE rowNumberInAllBlocks() < 1000)
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE t_nondet;
