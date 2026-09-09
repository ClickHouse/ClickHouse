-- `max_rows_to_read` with `read_overflow_mode = 'throw'` is checked against the row estimate
-- the reading step announces, not only against the rows actually read. When read-in-order is
-- propagated through a `LEFT ALL`/`LEFT ANY` join, the LIMIT must bound that estimate
-- (such a join emits at least one output row per left row), otherwise these queries fail
-- although they read only a few rows. See `joinKeepsLeftSideLimit`.
-- The opposite (other join kinds announce the full table and throw) is not asserted here:
-- whether the early estimate check fires depends on how the read is split into sources,
-- which differs between storage types.

-- Pin everything that could disable the plain-hash-join through-join read order.
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_before_external_join = 0;
SET join_algorithm = 'hash';
SET optimize_read_in_order = 1;
SET query_plan_read_in_order = 1;
SET query_plan_read_in_order_through_join = 1;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_shard_by_pk_ranges = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_left_in_order;
DROP TABLE IF EXISTS t_right_dim;

-- Small granules, so the in-order read can stop far below `max_rows_to_read`
-- (a granule is the minimum read unit).
CREATE TABLE t_left_in_order (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 10;
CREATE TABLE t_right_dim (id UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_left_in_order SELECT number FROM numbers(1000);
INSERT INTO t_right_dim SELECT number % 5 FROM numbers(20); -- ids 0..4, each 4 times

-- LEFT keeps the limit: must not throw.
SELECT f.id FROM t_left_in_order AS f LEFT JOIN t_right_dim AS d ON f.id = d.id
ORDER BY f.id LIMIT 6
SETTINGS max_rows_to_read = 100, read_overflow_mode = 'throw';

SELECT f.id FROM t_left_in_order AS f ANY LEFT JOIN t_right_dim AS d ON f.id = d.id
ORDER BY f.id LIMIT 3
SETTINGS max_rows_to_read = 100, read_overflow_mode = 'throw';

DROP TABLE t_left_in_order;
DROP TABLE t_right_dim;
