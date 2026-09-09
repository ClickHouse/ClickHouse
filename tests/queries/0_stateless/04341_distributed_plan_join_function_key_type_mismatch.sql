-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

SET enable_parallel_replicas = 0;
-- Distributed aggregation cannot enforce a global max_rows_to_group_by, so pin it to 0.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t1_04341;
DROP TABLE IF EXISTS t2_04341;
CREATE TABLE t1_04341 (key UInt64, val Nullable(String)) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t2_04341 (key UInt64, val Nullable(String)) ENGINE = MergeTree ORDER BY key;
INSERT INTO t1_04341 SELECT number, toString(number) FROM numbers(100);
INSERT INTO t2_04341 SELECT number, toString(number) FROM numbers(100);

-- Function-wrapped key (`Int8`) with no common supertype with the other key (`UInt64`). Used to
-- abort with a `LOGICAL_ERROR` exception; now the distributed plan rejects the shuffle, and the
-- keys are joined by the values that they have in common.
SELECT DISTINCT t2_04341.val
FROM t1_04341 INNER JOIN t2_04341 ON intDiv(-1, t1_04341.key + 1) = t2_04341.key
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, serialize_query_plan = 1,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
    enable_parallel_replicas = 0;

-- The plan is distributed: the initiator pipeline reads from the distributed executor.
SELECT sum(explain LIKE '%ReadFromDistributedPlanSource%') > 0
FROM (
    EXPLAIN PIPELINE
    SELECT DISTINCT t2_04341.val
    FROM t1_04341 INNER JOIN t2_04341 ON intDiv(-1, t1_04341.key + 1) = t2_04341.key
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, serialize_query_plan = 1,
        distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
        enable_parallel_replicas = 0
);

-- A compatible function-wrapped key still distributes via shuffle and returns correct results.
-- The settings must be on the statement: a SETTINGS clause inside the subquery does not reach the
-- outer plan, which then runs locally without any distributed decision.
SELECT count() FROM (
    SELECT DISTINCT t2_04341.val
    FROM t1_04341 INNER JOIN t2_04341 ON intDiv(t1_04341.key, 2) = t2_04341.key
)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, serialize_query_plan = 1,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
    enable_parallel_replicas = 0;

SELECT sum(explain LIKE '%ReadFromDistributedPlanSource%') > 0
FROM (
    EXPLAIN PIPELINE
    SELECT count() FROM (
        SELECT DISTINCT t2_04341.val
        FROM t1_04341 INNER JOIN t2_04341 ON intDiv(t1_04341.key, 2) = t2_04341.key
    )
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, serialize_query_plan = 1,
        distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
        enable_parallel_replicas = 0
);

DROP TABLE t1_04341;
DROP TABLE t2_04341;
