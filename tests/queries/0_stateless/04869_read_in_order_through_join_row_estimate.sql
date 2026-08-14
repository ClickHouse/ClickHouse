-- `max_rows_to_read` with `read_overflow_mode = 'throw'` is checked against the number of rows the
-- reading step announces up front, not against the rows it ends up reading. When read-in-order is
-- propagated through a `LEFT JOIN`, the `LIMIT` still bounds how many rows the preserved side has
-- to produce, so the announcement must be bounded by it too - otherwise a read that stops after
-- one granule looks like a full scan to the row limits.

-- The announcement is delivered per source, and only when the source runs. In-order reading may
-- split a part into several sources (e.g. reading from object storage), and then a query that
-- stops early never runs the later sources, so their announcements are never delivered.
-- `max_threads = 1` keeps the whole part in a single source, whose announcement is delivered on
-- the first read on every storage. Parallel replicas skip the announcement altogether (the rows
-- would be counted once per replica), so they are disabled explicitly.

DROP TABLE IF EXISTS t_left_04869;
DROP TABLE IF EXISTS t_right_04869;

CREATE TABLE t_left_04869 (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 10;
CREATE TABLE t_right_04869 (id UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_left_04869 SELECT number FROM numbers(1000);
INSERT INTO t_right_04869 SELECT number FROM numbers(10);

-- The join is wrapped in `SpillingHashJoin` by the default `max_bytes_ratio_before_external_join`,
-- so this is the plan the second-pass through-join read-in-order produces by default.
SELECT l.id
FROM t_left_04869 AS l
LEFT JOIN t_right_04869 AS r ON l.id = r.id
ORDER BY l.id
LIMIT 1
SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1,
    query_plan_read_in_order_through_join = 1, read_in_order_use_virtual_row = 1,
    query_plan_join_swap_table = 0, max_block_size = 10,
    max_threads = 1, enable_parallel_replicas = 0,
    max_rows_to_read = 100, read_overflow_mode = 'throw',
    collect_hash_table_stats_during_joins = 0, enable_join_runtime_filters = 0;

-- Without the automatic spill threshold the join is a plain `HashJoin`, which takes the same path.
SELECT l.id
FROM t_left_04869 AS l
LEFT JOIN t_right_04869 AS r ON l.id = r.id
ORDER BY l.id
LIMIT 1
SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1,
    query_plan_read_in_order_through_join = 1, read_in_order_use_virtual_row = 1,
    query_plan_join_swap_table = 0, max_block_size = 10,
    max_threads = 1, enable_parallel_replicas = 0,
    max_rows_to_read = 100, read_overflow_mode = 'throw',
    collect_hash_table_stats_during_joins = 0, enable_join_runtime_filters = 0,
    max_bytes_ratio_before_external_join = 0, max_bytes_before_external_join = 0;

-- An `INNER JOIN` can drop rows of the left side, so the `LIMIT` does not bound how much of it has
-- to be read and the announcement stays at the full part. This is the conservative estimate, not a
-- statement that the query would really read that much.
SELECT l.id
FROM t_left_04869 AS l
INNER JOIN t_right_04869 AS r ON l.id = r.id
ORDER BY l.id
LIMIT 1
SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1,
    query_plan_read_in_order_through_join = 1, read_in_order_use_virtual_row = 1,
    query_plan_join_swap_table = 0, max_block_size = 10,
    max_threads = 1, enable_parallel_replicas = 0,
    max_rows_to_read = 100, read_overflow_mode = 'throw',
    collect_hash_table_stats_during_joins = 0, enable_join_runtime_filters = 0,
    max_bytes_ratio_before_external_join = 0, max_bytes_before_external_join = 0; -- { serverError TOO_MANY_ROWS }

-- A filter above the join still drops the limit: it can reject every row the reading step produces.
SELECT l.id
FROM t_left_04869 AS l
LEFT JOIN t_right_04869 AS r ON l.id = r.id
WHERE l.id % 997 = 996
ORDER BY l.id
LIMIT 1
SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1,
    query_plan_read_in_order_through_join = 1, read_in_order_use_virtual_row = 1,
    query_plan_join_swap_table = 0, max_block_size = 10,
    max_threads = 1, enable_parallel_replicas = 0,
    max_rows_to_read = 100, read_overflow_mode = 'throw',
    collect_hash_table_stats_during_joins = 0, enable_join_runtime_filters = 0,
    max_bytes_ratio_before_external_join = 0, max_bytes_before_external_join = 0; -- { serverError TOO_MANY_ROWS }

DROP TABLE t_left_04869;
DROP TABLE t_right_04869;
