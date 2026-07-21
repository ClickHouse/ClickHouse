-- Test SpillingHashJoin: automatic spilling of hash joins to disk.

SET max_bytes_before_external_join = 1000000000;
SET grace_hash_join_initial_buckets = 1;

-- Ensure it shows up in query plan nicely
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN actions = 1 SELECT * FROM numbers(1) AS t1 JOIN numbers(1) AS t2 ON t1.number = t2.number SETTINGS join_algorithm = 'hash'
)
WHERE trim(explain) LIKE 'Algorithm%';
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN actions = 1 SELECT * FROM numbers(1) AS t1 JOIN numbers(1) AS t2 ON t1.number = t2.number SETTINGS join_algorithm = 'parallel_hash'
)
WHERE trim(explain) LIKE 'Algorithm%';

SET join_algorithm = 'hash';
SET max_threads = 1;
-- Test 1: Small INNER JOIN that fits in memory.
SELECT 'inner join small';
SELECT count(), sum(t2.v)
FROM (SELECT number AS k FROM numbers(100)) AS t1
INNER JOIN (SELECT number AS k, number * 10 AS v FROM numbers(100)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_01';

-- Test 2: Small LEFT JOIN that fits in memory.
SELECT 'left join small';
SELECT count(), sum(if(t2.k = 0 AND t1.k != 0, 0, t2.v))
FROM (SELECT number AS k FROM numbers(100)) AS t1
LEFT JOIN (SELECT number + 50 AS k, number AS v FROM numbers(100)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_02';

-- Test 3: Small RIGHT JOIN that fits in memory.
SELECT 'right join small';
SELECT count()
FROM (SELECT number + 50 AS k FROM numbers(100)) AS t1
RIGHT JOIN (SELECT number AS k FROM numbers(100)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_03';

-- Test 4: Small FULL JOIN that fits in memory.
SELECT 'full join small';
SELECT count()
FROM (SELECT number AS k FROM numbers(100)) AS t1
FULL JOIN (SELECT number + 50 AS k FROM numbers(100)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_04';

-- Test 5: INNER JOIN that exceeds max_bytes_before_external_join and must spill.
SELECT 'inner join spill';
SET max_bytes_before_external_join = 100000;
SELECT count(), sum(t2.v)
FROM (SELECT number AS k FROM numbers(10000)) AS t1
INNER JOIN (SELECT number AS k, number AS v FROM numbers(10000)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_05';

-- Test 6: LEFT JOIN that spills.
SELECT 'left join spill';
SELECT count()
FROM (SELECT number AS k FROM numbers(10000)) AS t1
LEFT JOIN (SELECT number + 5000 AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_06';

-- Test 7: RIGHT JOIN that spills.
SELECT 'right join spill';
SELECT count()
FROM (SELECT number + 5000 AS k FROM numbers(10000)) AS t1
RIGHT JOIN (SELECT number AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_07';

-- Test 8: FULL JOIN that spills.
SELECT 'full join spill';
SELECT count()
FROM (SELECT number AS k FROM numbers(10000)) AS t1
FULL JOIN (SELECT number + 5000 AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_08';

-- Test 9: RIGHT JOIN that spills — verify non-joined rows from right table.
-- t1.k = [5000..14999], t2.k = [0..9999]
-- Matched: 5000, Non-joined from right: 5000, Total: 10000
SELECT 'right join spill non-joined';
SELECT count(), countIf(t1.k != 0) AS matched, countIf(t1.k = 0) AS right_only
FROM (SELECT number + 5000 AS k FROM numbers(10000)) AS t1
RIGHT JOIN (SELECT number AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_09';

-- Test 10: FULL JOIN that spills — verify non-joined rows from both sides.
-- t1.k = [1..10000], t2.k = [5001..15000]
-- Matched: 5000, Left-only: 5000, Right-only: 5000, Total: 15000
SELECT 'full join spill non-joined';
SELECT count(), countIf(t1.k != 0 AND t2.k != 0) AS matched,
       countIf(t2.k = 0) AS left_only, countIf(t1.k = 0) AS right_only
FROM (SELECT number + 1 AS k FROM numbers(10000)) AS t1
FULL JOIN (SELECT number + 5001 AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_10';

-- ====================================================================
-- Concurrent path: SpillingHashJoin wrapping ConcurrentHashJoin.
-- ====================================================================
SET max_bytes_before_external_join = 1000000000;
SET join_algorithm = 'parallel_hash';
SET max_threads = 4;

-- Test 11: Small INNER JOIN that fits in memory (concurrent, no spill).
SELECT 'concurrent inner join small';
SELECT count(), sum(t2.v)
FROM (SELECT number AS k FROM numbers(100)) AS t1
INNER JOIN (SELECT number AS k, number * 10 AS v FROM numbers(100)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_11';

-- Test 12: Small LEFT JOIN that fits in memory (concurrent, no spill).
SELECT 'concurrent left join small';
SELECT count(), sum(if(t2.k = 0 AND t1.k != 0, 0, t2.v))
FROM (SELECT number AS k FROM numbers(100)) AS t1
LEFT JOIN (SELECT number + 50 AS k, number AS v FROM numbers(100)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_12';

-- Test 13: Small RIGHT JOIN that fits in memory (concurrent, no spill).
SELECT 'concurrent right join small';
SELECT count()
FROM (SELECT number + 50 AS k FROM numbers(100)) AS t1
RIGHT JOIN (SELECT number AS k FROM numbers(100)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_13';

-- Test 14: Small FULL JOIN that fits in memory (concurrent, no spill).
SELECT 'concurrent full join small';
SELECT count()
FROM (SELECT number AS k FROM numbers(100)) AS t1
FULL JOIN (SELECT number + 50 AS k FROM numbers(100)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_14';

-- Test 15: Concurrent RIGHT JOIN (in-memory) — verify non-joined rows from right table.
-- t1.k = [5000..14999], t2.k = [0..9999]
-- Matched: 5000, Non-joined from right: 5000, Total: 10000
SELECT 'concurrent right join non-joined';
SELECT count(), countIf(t1.k != 0) AS matched, countIf(t1.k = 0) AS right_only
FROM (SELECT number + 5000 AS k FROM numbers(10000)) AS t1
RIGHT JOIN (SELECT number AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_15';

-- Test 16: Concurrent FULL JOIN (in-memory) — verify non-joined rows from both sides.
-- t1.k = [1..10000], t2.k = [5001..15000]
-- Matched: 5000, Left-only: 5000, Right-only: 5000, Total: 15000
SELECT 'concurrent full join non-joined';
SELECT count(), countIf(t1.k != 0 AND t2.k != 0 ) AS matched,
       countIf(t2.k = 0) AS left_only, countIf(t1.k = 0) AS right_only
FROM (SELECT number + 1 AS k FROM numbers(10000)) AS t1
FULL JOIN (SELECT number + 5001 AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_16';

SET max_bytes_before_external_join = 100000;
-- Increase initial bucket size to ensure delayed blocks are handled
SET grace_hash_join_initial_buckets = 2;
-- Test 17: INNER JOIN that exceeds max_bytes_before_external_join and must spill (concurrent).
SELECT 'concurrent inner join spill';
SELECT count(), sum(t2.v)
FROM (SELECT number AS k FROM numbers(10000)) AS t1
INNER JOIN (SELECT number AS k, number AS v FROM numbers(10000)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_17';

-- Test 18: LEFT JOIN that spills (concurrent).
SELECT 'concurrent left join spill';
SELECT count()
FROM (SELECT number AS k FROM numbers(10000)) AS t1
LEFT JOIN (SELECT number + 5000 AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_18';

-- Test 19: RIGHT JOIN that spills (concurrent).
SELECT 'concurrent right join spill';
SELECT count()
FROM (SELECT number + 5000 AS k FROM numbers(10000)) AS t1
RIGHT JOIN (SELECT number AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_19';

-- Test 20: FULL JOIN that spills (concurrent).
SELECT 'concurrent full join spill';
SELECT count()
FROM (SELECT number AS k FROM numbers(10000)) AS t1
FULL JOIN (SELECT number + 5000 AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_20';

-- Test 21: Concurrent RIGHT JOIN that spills — verify non-joined rows from right table.
-- t1.k = [5000..14999], t2.k = [0..9999]
-- Matched: 5000, Non-joined from right: 5000, Total: 10000
SELECT 'concurrent right join spill non-joined';
SELECT count(), countIf(t1.k != 0) AS matched, countIf(t1.k = 0) AS right_only
FROM (SELECT number + 5000 AS k FROM numbers(10000)) AS t1
RIGHT JOIN (SELECT number AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_21';

-- Test 22: Concurrent FULL JOIN that spills — verify non-joined rows from both sides.
-- t1.k = [1..10000], t2.k = [5001..15000]
-- Matched: 5000, Left-only: 5000, Right-only: 5000, Total: 15000
SELECT 'concurrent full join spill non-joined';
SELECT count(), countIf(t1.k != 0 AND t2.k != 0) AS matched,
       countIf(t2.k = 0) AS left_only, countIf(t1.k = 0) AS right_only
FROM (SELECT number + 1 AS k FROM numbers(10000)) AS t1
FULL JOIN (SELECT number + 5001 AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k
SETTINGS log_comment = 'query_03915_22';

-- ====================================================================
-- Profile event counters for non-joined and delayed blocks.
-- ====================================================================
SYSTEM FLUSH LOGS query_log;

SELECT 'profile events';
SELECT
    log_comment,
    ProfileEvents['JoinNonJoinedTransformBlockCount'] > 0 AS has_non_joined_blocks,
    ProfileEvents['JoinNonJoinedTransformRowCount'] AS non_joined_rows,
    ProfileEvents['JoinDelayedJoinedTransformBlockCount'] > 0 AS has_delayed_blocks,
    ProfileEvents['JoinDelayedJoinedTransformRowCount'] > 0 AS has_delayed_rows,
    ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] AS spilled_to_disk
FROM system.query_log
WHERE log_comment LIKE 'query\_03915\_%' AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND event_date >= yesterday()
ORDER BY log_comment;
