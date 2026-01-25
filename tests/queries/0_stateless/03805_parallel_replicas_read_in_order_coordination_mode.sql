-- Tags: zookeeper

-- Test for the fix of coordination mode mismatch with parallel replicas and read-in-order optimization.
-- When a column is "fixed" (constant due to WHERE), the read-in-order optimization skips it when
-- determining read direction. With parallel_replicas_local_plan, different replicas could identify
-- different columns as fixed, causing coordination mode mismatch (WithOrder vs ReverseOrder).

DROP TABLE IF EXISTS t_read_in_order_coordination SYNC;

CREATE TABLE t_read_in_order_coordination
(
    tenant UInt32,
    event_time UInt32
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_read_in_order_coordination', 'r1')
ORDER BY (tenant, event_time);

-- Insert deterministic test data
INSERT INTO t_read_in_order_coordination SELECT 1, number FROM numbers(10);
INSERT INTO t_read_in_order_coordination SELECT 2, number FROM numbers(10);

SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET enable_parallel_replicas = 1;
SET parallel_replicas_local_plan = 1;
SET enable_analyzer = 1;

-- This query has:
-- 1. WHERE tenant = 1 - makes 'tenant' a fixed column
-- 2. ORDER BY tenant, event_time DESC
-- Without the fix, different replicas might compute different read directions:
-- - If 'tenant' is identified as fixed: direction comes from event_time DESC -> ReverseOrder
-- - If 'tenant' is not identified as fixed: direction comes from tenant ASC -> WithOrder
-- This would cause: "Replica N decided to read in WithOrder mode, not in ReverseOrder"
SELECT tenant, event_time
FROM t_read_in_order_coordination
WHERE tenant = 1
ORDER BY tenant, event_time DESC
LIMIT 5;

SELECT '---';

-- Also test ascending order with fixed column
SELECT tenant, event_time
FROM t_read_in_order_coordination
WHERE tenant = 2
ORDER BY tenant, event_time ASC
LIMIT 5;

DROP TABLE t_read_in_order_coordination SYNC;
