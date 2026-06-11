-- `GROUP BY ... LIMIT` without `ORDER BY` (Pattern 2) has no downstream sort to
-- rank stale partially-aggregated groups below complete ones, so the heap is
-- only sound when evicted keys are erased from the hash table.  The optimizer
-- marks Pattern 2 with `requires_pruning`, and the aggregator disables the heap
-- at runtime for methods whose hash table cannot erase.
--
-- Any LIMIT-many groups are a valid Pattern 2 answer, so the checks below do
-- not pin which groups are returned; they verify that every returned group
-- carries its complete aggregate (count of matches against the full,
-- non-optimized aggregation).

SET max_threads = 1;
-- The trivial analyzer pass handles `GROUP BY ... LIMIT` via `max_rows_to_group_by`
-- and hides the plan-level Pattern 2; disable it to exercise the heap path.
SET optimize_trivial_group_by_limit_query = 0;

SELECT 'Pattern 2 plan is marked with requires_pruning';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT k FROM (SELECT number % 100 AS k FROM numbers(1000)) GROUP BY k LIMIT 5 SETTINGS enable_group_by_top_k_optimization = 1) WHERE explain LIKE '%Top-K%';

SELECT 'Pattern 1 plan is not';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT k FROM (SELECT number % 100 AS k FROM numbers(1000)) GROUP BY k ORDER BY k LIMIT 5 SETTINGS enable_group_by_top_k_optimization = 1) WHERE explain LIKE '%Top-K%';

SELECT 'UInt32 key (erasable hash table, heap + pruning active)';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT k, sum(v) AS s FROM (SELECT toUInt32(999 - (number % 1000)) AS k, 1 AS v FROM numbers(4000)) GROUP BY k LIMIT 5 SETTINGS enable_group_by_top_k_optimization = 1) AS l
    INNER JOIN (SELECT k, sum(v) AS s FROM (SELECT toUInt32(999 - (number % 1000)) AS k, 1 AS v FROM numbers(4000)) GROUP BY k SETTINGS enable_group_by_top_k_optimization = 0) AS f USING (k)
);

SELECT 'Composite key (pruning of composite heaps)';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT a, b, sum(v) AS s FROM (SELECT toUInt32(99 - intDiv(number % 1000, 10)) AS a, toString(number % 10) AS b, 1 AS v FROM numbers(4000)) GROUP BY a, b LIMIT 5 SETTINGS enable_group_by_top_k_optimization = 1) AS l
    INNER JOIN (SELECT a, b, sum(v) AS s FROM (SELECT toUInt32(99 - intDiv(number % 1000, 10)) AS a, toString(number % 10) AS b, 1 AS v FROM numbers(4000)) GROUP BY a, b SETTINGS enable_group_by_top_k_optimization = 0) AS f USING (a, b)
);

SELECT 'String key (StringHashTable supports erase)';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT k, sum(v) AS s FROM (SELECT leftPad(toString(999 - (number % 1000)), 4, '0') AS k, 1 AS v FROM numbers(4000)) GROUP BY k LIMIT 5 SETTINGS enable_group_by_top_k_optimization = 1) AS l
    INNER JOIN (SELECT k, sum(v) AS s FROM (SELECT leftPad(toString(999 - (number % 1000)), 4, '0') AS k, 1 AS v FROM numbers(4000)) GROUP BY k SETTINGS enable_group_by_top_k_optimization = 0) AS f USING (k)
);

SELECT 'UInt8 key (FixedHashTable cannot erase, heap disabled at runtime)';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT k, sum(v) AS s FROM (SELECT toUInt8(255 - (number % 256)) AS k, 1 AS v FROM numbers(1024)) GROUP BY k LIMIT 5 SETTINGS enable_group_by_top_k_optimization = 1) AS l
    INNER JOIN (SELECT k, sum(v) AS s FROM (SELECT toUInt8(255 - (number % 256)) AS k, 1 AS v FROM numbers(1024)) GROUP BY k SETTINGS enable_group_by_top_k_optimization = 0) AS f USING (k)
);

SELECT 'Date key (key16, FixedHashTable cannot erase, heap disabled at runtime)';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT k, sum(v) AS s FROM (SELECT toDate('2020-01-01') + (999 - (number % 1000)) AS k, 1 AS v FROM numbers(4000)) GROUP BY k LIMIT 5 SETTINGS enable_group_by_top_k_optimization = 1) AS l
    INNER JOIN (SELECT k, sum(v) AS s FROM (SELECT toDate('2020-01-01') + (999 - (number % 1000)) AS k, 1 AS v FROM numbers(4000)) GROUP BY k SETTINGS enable_group_by_top_k_optimization = 0) AS f USING (k)
);
