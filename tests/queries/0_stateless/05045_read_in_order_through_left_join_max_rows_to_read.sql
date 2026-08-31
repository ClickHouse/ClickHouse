-- `max_rows_to_read` with `read_overflow_mode = 'throw'` is checked against the row estimate
-- the reading step announces, not against the rows actually read. When read-in-order is
-- propagated through a join, the LIMIT bounds that estimate only for `LEFT ALL`/`LEFT ANY`
-- joins (they emit at least one output row per left row). Other kinds can drop left rows,
-- so the full-table estimate remains and still throws. See `joinKeepsLeftSideLimit`.

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

CREATE TABLE t_left_in_order (id UInt64) ENGINE = MergeTree ORDER BY id;
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

-- INNER can drop left rows: the limit is ignored and the full-table estimate throws.
SELECT f.id FROM t_left_in_order AS f INNER JOIN t_right_dim AS d ON f.id = d.id
ORDER BY f.id LIMIT 1
SETTINGS max_rows_to_read = 100, read_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }

DROP TABLE t_left_in_order;
DROP TABLE t_right_dim;
