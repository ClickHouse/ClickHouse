-- The bucket top-K plan optimization: when a final aggregation feeds `ORDER BY` over its
-- outputs with `LIMIT n` and the per-bucket selection is provably exact (today the rule fires
-- for a single sort column tracing to the aggregation's lone `count()`), the conversion
-- materializes only each two-level bucket's best n groups. The cells assert the pushed flag through the `Bucket top-K` line of
-- `EXPLAIN actions = 1` (presence only), plus one exactness pair. The two plan settings the
-- rewrite depends on are pinned against the runner's randomization.

SET query_plan_enable_optimizations = 1;
SET query_plan_push_down_limit = 1;
SET query_plan_aggregation_bucket_top_k = 1;

-- The matching shape carries the flag.
SELECT count() FROM (EXPLAIN actions = 1 SELECT number % 1000 AS k, count() AS c FROM numbers(100000) GROUP BY k ORDER BY c DESC LIMIT 10)
WHERE explain LIKE '%Bucket top-K: 10 descending%';

-- Ascending order is pushed with its direction.
SELECT count() FROM (EXPLAIN actions = 1 SELECT number % 1000 AS k, count() AS c FROM numbers(100000) GROUP BY k ORDER BY c ASC LIMIT 10)
WHERE explain LIKE '%Bucket top-K: 10 ascending%';

-- The dedicated setting disables the rewrite.
SELECT count() FROM (EXPLAIN actions = 1 SELECT number % 1000 AS k, count() AS c FROM numbers(100000) GROUP BY k ORDER BY c DESC LIMIT 10 SETTINGS query_plan_aggregation_bucket_top_k = 0)
WHERE explain LIKE '%Bucket top-K%';

-- `WITH TIES` cannot know the output size in advance, so the rewrite must not fire.
SELECT count() FROM (EXPLAIN actions = 1 SELECT number % 1000 AS k, count() AS c FROM numbers(100000) GROUP BY k ORDER BY c DESC LIMIT 10 WITH TIES)
WHERE explain LIKE '%Bucket top-K%';

-- Sorting by the key instead of the count must not fire either.
SELECT count() FROM (EXPLAIN actions = 1 SELECT number % 1000 AS k, count() AS c FROM numbers(100000) GROUP BY k ORDER BY k DESC LIMIT 10)
WHERE explain LIKE '%Bucket top-K%';

-- Exactness: the group counts are all distinct (floor(sqrt(x)) = i covers 2i + 1 integers), so
-- the top-5 is unique and must match with the rewrite on and off.
SELECT k, c FROM (SELECT toUInt32(sqrt(number)) AS k, count() AS c FROM numbers(1000000) GROUP BY k ORDER BY c DESC LIMIT 5)
ORDER BY c DESC, k;
SELECT k, c FROM (SELECT toUInt32(sqrt(number)) AS k, count() AS c FROM numbers(1000000) GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS query_plan_aggregation_bucket_top_k = 0)
ORDER BY c DESC, k;
