-- A join over two joins that output no columns has an input named `__join_result_dummy` on each side.
-- Every distributed plan task is serialized, so such a join has to survive a round trip.
-- Each distributed query is followed by the same query without the distributed plan.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t5;
DROP TABLE IF EXISTS t_empty;

CREATE TABLE t1 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t1 VALUES (1, 2), (3, 4), (5, 6);

CREATE TABLE t2 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t2 VALUES (3, 4), (5, 6), (7, 8);

CREATE TABLE t3 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t3 VALUES (5, 6), (7, 8), (9, 10);

CREATE TABLE t4 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t4 VALUES (7, 8), (9, 10), (11, 12);

CREATE TABLE t5 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t5 VALUES (9, 10), (11, 12), (13, 14);

CREATE TABLE t_empty (id UInt64) ENGINE = MergeTree ORDER BY id;

SET enable_parallel_replicas = 0, max_rows_to_group_by = 0, query_plan_use_new_logical_join_step = 1,
    distributed_plan_execute_locally = 1, distributed_plan_workers_num = 2, distributed_plan_fallback_to_local_execution = 0,
    make_distributed_plan = 0;

-- `query_plan_optimize_join_order_limit` 0 and 1 keep both children of the top join dummy-only.
SELECT count() FROM t1, t2, t3, t4 WHERE (t1.b = t2.b) AND (t3.a = t4.a)
SETTINGS make_distributed_plan = 1, query_plan_optimize_join_order_limit = 0, enable_cascades_optimizer = 0, log_comment = '05363_distributed';
SELECT count() FROM t1, t2, t3, t4 WHERE (t1.b = t2.b) AND (t3.a = t4.a)
SETTINGS query_plan_optimize_join_order_limit = 0, enable_cascades_optimizer = 0;

SELECT count() FROM t1, t2, t3, t4 WHERE (t1.b = t2.b) AND (t3.a = t4.a)
SETTINGS make_distributed_plan = 1, query_plan_optimize_join_order_limit = 1, enable_cascades_optimizer = 0, log_comment = '05363_distributed';
SELECT count() FROM t1, t2, t3, t4 WHERE (t1.b = t2.b) AND (t3.a = t4.a)
SETTINGS query_plan_optimize_join_order_limit = 1, enable_cascades_optimizer = 0;

SELECT count() FROM t1, t2, t3, t4 WHERE (t1.b = t2.b) AND (t3.a = t4.a)
SETTINGS make_distributed_plan = 1, query_plan_optimize_join_order_limit = 0, enable_cascades_optimizer = 1, log_comment = '05363_distributed';
SELECT count() FROM t1, t2, t3, t4 WHERE (t1.b = t2.b) AND (t3.a = t4.a)
SETTINGS query_plan_optimize_join_order_limit = 0, enable_cascades_optimizer = 1;

SELECT count() FROM t1, t2, t3, t4 WHERE (t1.b = t2.b) AND (t3.a = t4.a)
SETTINGS make_distributed_plan = 1, query_plan_optimize_join_order_limit = 1, enable_cascades_optimizer = 1, log_comment = '05363_distributed';
SELECT count() FROM t1, t2, t3, t4 WHERE (t1.b = t2.b) AND (t3.a = t4.a)
SETTINGS query_plan_optimize_join_order_limit = 1, enable_cascades_optimizer = 1;

-- A table joined on top of the join with two dummy-only children.
SELECT count(), max(t5.b) FROM t3, t4, t1, t2, t5 WHERE (t1.b = t2.b) AND (t3.a = t4.a)
SETTINGS make_distributed_plan = 1, query_plan_optimize_join_order_limit = 1, enable_cascades_optimizer = 1, log_comment = '05363_distributed';
SELECT count(), max(t5.b) FROM t3, t4, t1, t2, t5 WHERE (t1.b = t2.b) AND (t3.a = t4.a)
SETTINGS query_plan_optimize_join_order_limit = 1, enable_cascades_optimizer = 1;

SELECT count() FROM t_empty AS a, t_empty AS b, t_empty AS c, t_empty AS d WHERE (a.id = b.id + 1) AND (c.id = d.id + 1)
SETTINGS make_distributed_plan = 1, query_plan_optimize_join_order_limit = 1, enable_cascades_optimizer = 1, log_comment = '05363_distributed';
SELECT count() FROM t_empty AS a, t_empty AS b, t_empty AS c, t_empty AS d WHERE (a.id = b.id + 1) AND (c.id = d.id + 1)
SETTINGS query_plan_optimize_join_order_limit = 1, enable_cascades_optimizer = 1;

SYSTEM FLUSH LOGS query_log;
SELECT count(), min(ProfileEvents['DistributedPlanLocalExecution'])
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05363_distributed' AND type = 'QueryFinish' AND is_initial_query;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
DROP TABLE t_empty;
