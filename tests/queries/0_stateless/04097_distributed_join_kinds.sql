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
-- A nonzero max_rows_to_group_by keeps aggregation single-node, so pin it to 0.
SET max_rows_to_group_by = 0;
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


-- ASOF JOIN: broadcast (equality predicate is sufficient for shuffle partitioning,
-- the HashJoin ASOF implementation sorts the right side internally per equality-key
-- bucket, so input order after shuffle does not matter)
DROP TABLE IF EXISTS dist_trades;
DROP TABLE IF EXISTS dist_quotes;

CREATE TABLE dist_trades (symbol String, ts DateTime, price Decimal(10, 2))
ENGINE = MergeTree ORDER BY (symbol, ts)
SETTINGS index_granularity = 8192, auto_statistics_types = '';

CREATE TABLE dist_quotes (symbol String, ts DateTime, bid Decimal(10, 2))
ENGINE = MergeTree ORDER BY (symbol, ts)
SETTINGS index_granularity = 8192, auto_statistics_types = '';

-- Multiple symbols across multiple parts to exercise shuffle partitioning.
SYSTEM STOP MERGES dist_trades;
INSERT INTO dist_trades SELECT 'S' || toString(number % 5), toDateTime('2024-01-01 10:00:00') + number * 60, toDecimal64(100 + number * 0.1, 2) FROM numbers(100);
INSERT INTO dist_trades SELECT 'S' || toString(number % 5), toDateTime('2024-01-01 10:00:00') + (number + 100) * 60, toDecimal64(100 + (number + 100) * 0.1, 2) FROM numbers(100);

SYSTEM STOP MERGES dist_quotes;
INSERT INTO dist_quotes SELECT 'S' || toString(number % 5), toDateTime('2024-01-01 09:58:00') + number * 30, toDecimal64(99.5 + number * 0.05, 2) FROM numbers(200);
INSERT INTO dist_quotes SELECT 'S' || toString(number % 5), toDateTime('2024-01-01 09:58:00') + (number + 200) * 30, toDecimal64(99.5 + (number + 200) * 0.05, 2) FROM numbers(200);

SELECT '-- ASOF JOIN (broadcast)';
EXPLAIN PLAN SELECT symbol, count(), sum(price), sum(bid)
FROM dist_trades ASOF LEFT JOIN dist_quotes ON dist_trades.symbol = dist_quotes.symbol AND dist_trades.ts >= dist_quotes.ts
GROUP BY symbol ORDER BY symbol;

SELECT symbol, count(), sum(price), sum(bid)
FROM dist_trades ASOF LEFT JOIN dist_quotes ON dist_trades.symbol = dist_quotes.symbol AND dist_trades.ts >= dist_quotes.ts
GROUP BY symbol ORDER BY symbol;

-- Force shuffle by setting broadcast threshold to 0.
SELECT '-- ASOF JOIN (shuffle)';
EXPLAIN PLAN SELECT symbol, count(), sum(price), sum(bid)
FROM dist_trades ASOF LEFT JOIN dist_quotes ON dist_trades.symbol = dist_quotes.symbol AND dist_trades.ts >= dist_quotes.ts
GROUP BY symbol ORDER BY symbol
SETTINGS distributed_plan_max_rows_to_broadcast = 0;

SELECT symbol, count(), sum(price), sum(bid)
FROM dist_trades ASOF LEFT JOIN dist_quotes ON dist_trades.symbol = dist_quotes.symbol AND dist_trades.ts >= dist_quotes.ts
GROUP BY symbol ORDER BY symbol
SETTINGS distributed_plan_max_rows_to_broadcast = 0;

-- Single-node baseline.
SELECT symbol, count(), sum(price), sum(bid)
FROM dist_trades ASOF LEFT JOIN dist_quotes ON dist_trades.symbol = dist_quotes.symbol AND dist_trades.ts >= dist_quotes.ts
GROUP BY symbol ORDER BY symbol
SETTINGS make_distributed_plan = 0;

DROP TABLE dist_trades;
DROP TABLE dist_quotes;

DROP TABLE dist_orders;
DROP TABLE dist_items;
