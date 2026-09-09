-- Regression test for issue #116849: with `optimize_aggregation_in_order_limit`
-- enabled, a query whose `ORDER BY` is a strict prefix of the `GROUP BY` key
-- returned incomplete aggregate values when groups tie on that prefix and a
-- group's rows span more than one part. The limit must not be pushed into the
-- in-order aggregation unless the `ORDER BY` covers the full `GROUP BY` key.

DROP TABLE IF EXISTS t_agg_in_order_limit_prefix;

CREATE TABLE t_agg_in_order_limit_prefix (a UInt32, b UInt32, x UInt32)
ENGINE = MergeTree ORDER BY (a, b);

SYSTEM STOP MERGES t_agg_in_order_limit_prefix;

-- Two parts; groups with b in 4..20 span both parts.
INSERT INTO t_agg_in_order_limit_prefix SELECT 1, number, 10 FROM numbers(1, 20);
INSERT INTO t_agg_in_order_limit_prefix SELECT 1, number, 1 FROM numbers(4, 17);

-- Ground truth: sum(x) is 10 for groups with b in 1..3 and 11 for groups with b in 4..20.
-- All groups tie on `a`, so any three groups are a legal answer; assert that every
-- returned group carries its complete aggregate value. The check is done in the
-- projection on purpose: wrapping the query into a subquery changes the plan
-- (the aggregation is no longer executed in order), which hides the bug. The block
-- settings are pinned because tiny blocks also hide it.
SELECT sum(x) = if(b <= 3, 10, 11)
FROM t_agg_in_order_limit_prefix
GROUP BY a, b
ORDER BY a
LIMIT 3
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1,
         max_block_size = 65409, aggregation_in_order_max_block_bytes = 50000000;

-- Same with OFFSET.
SELECT sum(x) = if(b <= 3, 10, 11)
FROM t_agg_in_order_limit_prefix
GROUP BY a, b
ORDER BY a
LIMIT 3 OFFSET 2
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1,
         max_block_size = 65409, aggregation_in_order_max_block_bytes = 50000000;

-- The full-key `ORDER BY` still admits the push-down and stays correct.
SELECT a, b, sum(x)
FROM t_agg_in_order_limit_prefix
GROUP BY a, b
ORDER BY a, b
LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1;

DROP TABLE t_agg_in_order_limit_prefix;
