-- Pattern 2 (`GROUP BY ... LIMIT` without `ORDER BY`) is only sound when evicted keys
-- are erased from the hash table; the heap is disabled at runtime for methods that cannot.

SET max_threads = 1;
SET max_rows_to_group_by = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET optimize_trivial_group_by_limit_query = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

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

-- Pruning erases evicted keys, which is only globally correct when every row for a
-- key reaches the same heap.  With independent non-sharded streams a key pruned in
-- one stream but kept in another would surface incomplete; the heap is disabled for
-- multi-stream Pattern 2 (single-stream and sharded stay optimized).
SELECT 'Multi-stream: every returned group complete';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT k, sum(v) AS s FROM (SELECT (999 - number % 1000)::UInt32 AS k, 1 AS v FROM numbers_mt(2000000)) GROUP BY k LIMIT 3 SETTINGS enable_group_by_top_k_optimization = 1, max_threads = 8, enable_sharding_aggregator = 0) AS l
    INNER JOIN (SELECT k, sum(v) AS s FROM (SELECT (999 - number % 1000)::UInt32 AS k, 1 AS v FROM numbers_mt(2000000)) GROUP BY k SETTINGS enable_group_by_top_k_optimization = 0) AS f USING (k)
);

SELECT k FROM (SELECT (999 - number % 1000)::UInt32 AS k FROM numbers_mt(2000000)) GROUP BY k LIMIT 3
SETTINGS enable_group_by_top_k_optimization = 1, max_threads = 8, enable_sharding_aggregator = 0, log_comment = '04336_p2_multi' FORMAT Null;
SELECT k FROM (SELECT (999 - number % 1000)::UInt32 AS k FROM numbers(2000000)) GROUP BY k LIMIT 3
SETTINGS enable_group_by_top_k_optimization = 1, max_threads = 1, log_comment = '04336_p2_single' FORMAT Null;
SYSTEM FLUSH LOGS query_log;

SELECT 'Multi-stream heap disabled, single-stream heap active';
SELECT
    sumIf(ProfileEvents['AggregationTopKRowsSkipped'], log_comment = '04336_p2_multi') = 0 AS multi_heap_disabled,
    sumIf(ProfileEvents['AggregationTopKRowsSkipped'], log_comment = '04336_p2_single') > 0 AS single_heap_active
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('04336_p2_multi', '04336_p2_single');
