-- The right side of the cross join is a hash join output with lazily replicated (`ColumnReplicated`) columns:
-- `ConstantJoin` stores them as is and the emit loop reads them through the replication indexes.
-- `query_plan_optimize_join_order_limit = 1` keeps the hash join on the build side of the cross join,
-- and disabled squashing keeps its output blocks (and their replicated columns) intact.

SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 1;
SET min_joined_block_size_rows = 0;
SET min_joined_block_size_bytes = 0;
SET enable_lazy_columns_replication = 1;

DROP TABLE IF EXISTS t_strings;
DROP TABLE IF EXISTS t_many;
CREATE TABLE t_strings (k UInt64, s String) ENGINE = Memory;
CREATE TABLE t_many (k UInt64) ENGINE = Memory;
INSERT INTO t_strings SELECT number, concat('str_', toString(number), repeat('x', 64)) FROM numbers(10);
INSERT INTO t_many SELECT number % 10 FROM numbers(1000);

SELECT count(), uniqExact(s), sum(length(s))
FROM numbers(3) AS l
CROSS JOIN (SELECT t1.s AS s FROM t_strings AS t1 INNER JOIN t_many AS t2 ON t1.k = t2.k) AS r;

DROP TABLE t_strings;
DROP TABLE t_many;
