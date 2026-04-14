-- Test that the old heuristic-based distributed join (tryMakeDistributedJoin)
-- supports all join kinds and picks the correct distribution strategy.
--
-- dist_items (right side) has 400 rows, well below the broadcast threshold
-- (20000), so broadcast is chosen when safe.  For RIGHT and FULL joins,
-- broadcast is blocked because the right side can produce unmatched output
-- rows that would be duplicated across workers — shuffle is used instead.
--
-- Each join kind is tested with:
--   1. EXPLAIN to verify the chosen strategy (Broadcast vs Shuffle)
--   2. Distributed execution result
--   3. Single-node baseline for correctness comparison

SET enable_analyzer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET query_plan_use_new_logical_join_step = 1;
SET distributed_plan_default_shuffle_join_bucket_count = 4;
SET distributed_plan_force_exchange_kind = 'Persisted';
-- Pin settings that affect plan shape to make EXPLAIN output stable.
SET enable_join_runtime_filters = 0;
SET optimize_move_to_prewhere = 0;
SET query_plan_convert_outer_join_to_inner_join = 0;

DROP TABLE IF EXISTS dist_orders;
DROP TABLE IF EXISTS dist_items;

CREATE TABLE dist_orders (
    order_id UInt64,
    customer String
) ENGINE = MergeTree ORDER BY order_id
  SETTINGS index_granularity = 8192, auto_statistics_types = '';

CREATE TABLE dist_items (
    item_id UInt64,
    order_id UInt64,
    amount Decimal(10, 2)
) ENGINE = MergeTree ORDER BY item_id
  SETTINGS index_granularity = 8192, auto_statistics_types = '';

-- 4 parts for dist_orders: orders 0..399
SYSTEM STOP MERGES dist_orders;
INSERT INTO dist_orders SELECT number, 'C' || toString(number % 10) FROM numbers(100);
INSERT INTO dist_orders SELECT number + 100, 'C' || toString((number + 100) % 10) FROM numbers(100);
INSERT INTO dist_orders SELECT number + 200, 'C' || toString((number + 200) % 10) FROM numbers(100);
INSERT INTO dist_orders SELECT number + 300, 'C' || toString((number + 300) % 10) FROM numbers(100);

-- 4 parts for dist_items: items for orders 50..449 (overlap: 50..399 match, 400..449 don't)
SYSTEM STOP MERGES dist_items;
INSERT INTO dist_items SELECT number, number + 50, toDecimal64(number * 1.5, 2) FROM numbers(100);
INSERT INTO dist_items SELECT number + 100, number + 150, toDecimal64((number + 100) * 1.5, 2) FROM numbers(100);
INSERT INTO dist_items SELECT number + 200, number + 250, toDecimal64((number + 200) * 1.5, 2) FROM numbers(100);
INSERT INTO dist_items SELECT number + 300, number + 350, toDecimal64((number + 300) * 1.5, 2) FROM numbers(100);


-- INNER JOIN: broadcast (safe, right side small)
SELECT '-- INNER JOIN (broadcast)';
EXPLAIN PLAN SELECT count(), sum(amount)
FROM dist_orders INNER JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count(), sum(amount)
FROM dist_orders INNER JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count(), sum(amount)
FROM dist_orders INNER JOIN dist_items ON dist_orders.order_id = dist_items.order_id
SETTINGS make_distributed_plan = 0;


-- LEFT JOIN: broadcast (safe, right side small)
SELECT '-- LEFT JOIN (broadcast)';
EXPLAIN PLAN SELECT count()
FROM dist_orders LEFT JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders LEFT JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders LEFT JOIN dist_items ON dist_orders.order_id = dist_items.order_id
SETTINGS make_distributed_plan = 0;


-- RIGHT JOIN: shuffle (broadcast blocked — right side produces unmatched rows)
SELECT '-- RIGHT JOIN (shuffle)';
EXPLAIN PLAN SELECT count()
FROM dist_orders RIGHT JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders RIGHT JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders RIGHT JOIN dist_items ON dist_orders.order_id = dist_items.order_id
SETTINGS make_distributed_plan = 0;


-- FULL JOIN: shuffle (broadcast blocked — both sides produce unmatched rows)
SELECT '-- FULL JOIN (shuffle)';
EXPLAIN PLAN SELECT count()
FROM dist_orders FULL JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders FULL JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders FULL JOIN dist_items ON dist_orders.order_id = dist_items.order_id
SETTINGS make_distributed_plan = 0;


-- LEFT SEMI JOIN: broadcast (safe, right side small)
SELECT '-- LEFT SEMI JOIN (broadcast)';
EXPLAIN PLAN SELECT count()
FROM dist_orders LEFT SEMI JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders LEFT SEMI JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders LEFT SEMI JOIN dist_items ON dist_orders.order_id = dist_items.order_id
SETTINGS make_distributed_plan = 0;


-- LEFT ANTI JOIN: broadcast (safe, right side small)
SELECT '-- LEFT ANTI JOIN (broadcast)';
EXPLAIN PLAN SELECT count()
FROM dist_orders LEFT ANTI JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders LEFT ANTI JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders LEFT ANTI JOIN dist_items ON dist_orders.order_id = dist_items.order_id
SETTINGS make_distributed_plan = 0;


-- RIGHT SEMI JOIN: shuffle (broadcast blocked — kind is RIGHT)
SELECT '-- RIGHT SEMI JOIN (shuffle)';
EXPLAIN PLAN SELECT count()
FROM dist_orders RIGHT SEMI JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders RIGHT SEMI JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders RIGHT SEMI JOIN dist_items ON dist_orders.order_id = dist_items.order_id
SETTINGS make_distributed_plan = 0;


-- RIGHT ANTI JOIN: shuffle (broadcast blocked — kind is RIGHT)
SELECT '-- RIGHT ANTI JOIN (shuffle)';
EXPLAIN PLAN SELECT count()
FROM dist_orders RIGHT ANTI JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders RIGHT ANTI JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders RIGHT ANTI JOIN dist_items ON dist_orders.order_id = dist_items.order_id
SETTINGS make_distributed_plan = 0;


-- ANY INNER JOIN: broadcast (safe, right side small)
SELECT '-- ANY INNER JOIN (broadcast)';
EXPLAIN PLAN SELECT count()
FROM dist_orders ANY INNER JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders ANY INNER JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders ANY INNER JOIN dist_items ON dist_orders.order_id = dist_items.order_id
SETTINGS make_distributed_plan = 0;


-- ANY LEFT JOIN: broadcast (safe, right side small)
SELECT '-- ANY LEFT JOIN (broadcast)';
EXPLAIN PLAN SELECT count()
FROM dist_orders ANY LEFT JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders ANY LEFT JOIN dist_items ON dist_orders.order_id = dist_items.order_id;

SELECT count()
FROM dist_orders ANY LEFT JOIN dist_items ON dist_orders.order_id = dist_items.order_id
SETTINGS make_distributed_plan = 0;


DROP TABLE dist_orders;
DROP TABLE dist_items;
