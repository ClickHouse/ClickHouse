-- In a distributed query plan, `query_plan_join_shard_by_pk_ranges` used to make every node
-- read the whole table instead of only its slice, duplicating the join result across nodes.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'auto';
SET param__internal_cascades_cluster_node_count = 4;
-- The trigger: sharding by primary-key ranges applies only to a plain hash join
-- without automatic spilling to disk.
SET query_plan_join_shard_by_pk_ranges = 1;
SET join_algorithm = 'hash';
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;

DROP TABLE IF EXISTS t_orders_05046;
DROP TABLE IF EXISTS t_lineitem_05046;

CREATE TABLE t_orders_05046 (o_orderkey UInt64) ENGINE = MergeTree ORDER BY o_orderkey;
CREATE TABLE t_lineitem_05046 (l_orderkey UInt64) ENGINE = MergeTree ORDER BY l_orderkey;

INSERT INTO t_orders_05046 SELECT number FROM numbers(1000);
-- Only 20 unique keys matching orders 0..19, so the semi-join output is 20 rows.
INSERT INTO t_lineitem_05046 SELECT number FROM numbers(20);

-- Each count must be 20; a duplicated read returns node_count * 20 = 80.
SELECT count() FROM t_orders_05046
WHERE EXISTS (SELECT 1 FROM t_lineitem_05046 WHERE l_orderkey = o_orderkey);

SELECT count() FROM t_orders_05046 LEFT SEMI JOIN t_lineitem_05046 ON l_orderkey = o_orderkey;

SELECT count() FROM t_orders_05046 INNER JOIN t_lineitem_05046 ON l_orderkey = o_orderkey;

DROP TABLE t_orders_05046;
DROP TABLE t_lineitem_05046;
