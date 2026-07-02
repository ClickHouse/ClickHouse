-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer, like the other make_distributed_plan tests.

-- `PASTE JOIN` pairs rows by position, and no exchange preserves that order: a gather
-- over parallel reads interleaves worker streams arbitrarily. `make_distributed_plan`
-- rejects such plans up front (with and without Cascades) instead of returning wrongly
-- paired rows.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_paste_left;
DROP TABLE IF EXISTS t_paste_right;

CREATE TABLE t_paste_left (x UInt64) ENGINE = MergeTree() ORDER BY x;
CREATE TABLE t_paste_right (y UInt64) ENGINE = MergeTree() ORDER BY y;

INSERT INTO t_paste_left SELECT number FROM numbers(100000);
INSERT INTO t_paste_right SELECT number * 10 FROM numbers(1000);

SELECT '-- 1. PASTE JOIN is rejected under Cascades';
SELECT count() FROM (SELECT * FROM t_paste_left PASTE JOIN t_paste_right); -- { serverError SUPPORT_IS_DISABLED }

SELECT '-- 2. Baseline without Cascades';
SELECT count() FROM (SELECT * FROM t_paste_left PASTE JOIN t_paste_right)
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

SELECT '-- 3. PASTE JOIN nested under aggregation is also rejected';
SELECT sum(x + y) FROM (SELECT * FROM t_paste_left PASTE JOIN t_paste_right) WHERE x < 100; -- { serverError SUPPORT_IS_DISABLED }

SELECT '-- 4. Rejected without Cascades too (legacy path gathers a distributed read below the join)';
SELECT count() FROM (SELECT * FROM t_paste_left PASTE JOIN t_paste_right)
SETTINGS enable_cascades_optimizer = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_paste_left;
DROP TABLE t_paste_right;
