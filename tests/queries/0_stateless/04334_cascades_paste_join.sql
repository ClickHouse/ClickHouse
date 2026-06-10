-- `PASTE JOIN` pairs rows by position, so neither side may be partitioned
-- independently of the other. Broadcasting the right side while the left is
-- split across nodes pairs each left slice with the full right table.

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

SELECT '-- 1. PASTE JOIN: pairs rows by position, count = min(sides)';
SELECT count() FROM (SELECT * FROM t_paste_left PASTE JOIN t_paste_right);

SELECT '-- 2. Baseline without Cascades';
SELECT count() FROM (SELECT * FROM t_paste_left PASTE JOIN t_paste_right)
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE t_paste_left;
DROP TABLE t_paste_right;
