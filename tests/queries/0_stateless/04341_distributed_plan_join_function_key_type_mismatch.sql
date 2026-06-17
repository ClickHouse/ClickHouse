-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

DROP TABLE IF EXISTS t1_04341;
DROP TABLE IF EXISTS t2_04341;
CREATE TABLE t1_04341 (key UInt64, val Nullable(String)) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t2_04341 (key UInt64, val Nullable(String)) ENGINE = MergeTree ORDER BY key;
INSERT INTO t1_04341 SELECT number, toString(number) FROM numbers(100);
INSERT INTO t2_04341 SELECT number, toString(number) FROM numbers(100);

-- A function-wrapped join key whose result type (Int8) has no common supertype with the other key
-- (UInt64). The shuffle path used to call preCalculateKeys (which materializes the function key as
-- an extra input on the join step) and only then discover the keys are type-incompatible and bail,
-- leaving the join step with an input no child produces. Serializing that step for a distributed
-- stage then aborted in JoinExpressionActions with "Input nodes size mismatch in dag". Now the
-- shuffle is rejected before the mutation, the join runs single-node, and the genuine type
-- incompatibility surfaces as a clean NO_COMMON_TYPE error instead of a server crash.
SELECT DISTINCT t2_04341.val
FROM t1_04341 INNER JOIN t2_04341 ON intDiv(-1, t1_04341.key) = t2_04341.key
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, serialize_query_plan = 1,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
    enable_parallel_replicas = 0; -- { serverError NO_COMMON_TYPE }

-- A compatible function-wrapped key still distributes via shuffle and returns correct results.
SELECT count() FROM (
    SELECT DISTINCT t2_04341.val
    FROM t1_04341 INNER JOIN t2_04341 ON intDiv(t1_04341.key, 2) = t2_04341.key
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, serialize_query_plan = 1,
        distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
        enable_parallel_replicas = 0
);

DROP TABLE t1_04341;
DROP TABLE t2_04341;
