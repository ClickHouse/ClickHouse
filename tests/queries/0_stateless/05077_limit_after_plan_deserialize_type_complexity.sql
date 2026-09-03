-- Tags: distributed

-- A query plan received over the network deserializes the range boundaries under the same type complexity
-- limit as every other expression of the plan.
DROP TABLE IF EXISTS t_range_plan_local;
DROP TABLE IF EXISTS t_range_plan_dist;
CREATE TABLE t_range_plan_local (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_range_plan_local SELECT number FROM numbers(4);
CREATE TABLE t_range_plan_dist AS t_range_plan_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_range_plan_local);

SET serialize_query_plan = 1;
SET prefer_localhost_replica = 0;

SET input_format_binary_max_type_complexity = 2;
SELECT n FROM t_range_plan_dist ORDER BY n LIMIT 3 AFTER notEmpty([[[[[n]]]]]); -- { serverError INCORRECT_DATA }
SELECT n FROM t_range_plan_dist WHERE notEmpty([[[[[n]]]]]) ORDER BY n LIMIT 3; -- { serverError INCORRECT_DATA }

SET input_format_binary_max_type_complexity = 10;
SELECT n FROM t_range_plan_dist ORDER BY n LIMIT 3 AFTER notEmpty([[[[[n]]]]]);

DROP TABLE t_range_plan_dist;
DROP TABLE t_range_plan_local;
