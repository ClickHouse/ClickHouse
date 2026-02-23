-- Test SpillingHashJoin: automatic spilling of hash joins to disk.

SET join_algorithm = 'hash';
SET enable_auto_spilling_hash_join = 1;
SET max_threads = 1;

-- Test 1: Small INNER JOIN that fits in memory.
SELECT 'inner join small';
SELECT count(), sum(t2.v)
FROM (SELECT number AS k FROM numbers(100)) AS t1
INNER JOIN (SELECT number AS k, number * 10 AS v FROM numbers(100)) AS t2
ON t1.k = t2.k;

-- Test 2: Small LEFT JOIN that fits in memory.
SELECT 'left join small';
SELECT count(), sum(if(t2.k = 0 AND t1.k != 0, 0, t2.v))
FROM (SELECT number AS k FROM numbers(100)) AS t1
LEFT JOIN (SELECT number + 50 AS k, number AS v FROM numbers(100)) AS t2
ON t1.k = t2.k;

-- Test 3: Small RIGHT JOIN that fits in memory.
SELECT 'right join small';
SELECT count()
FROM (SELECT number + 50 AS k FROM numbers(100)) AS t1
RIGHT JOIN (SELECT number AS k FROM numbers(100)) AS t2
ON t1.k = t2.k;

-- Test 4: Small FULL JOIN that fits in memory.
SELECT 'full join small';
SELECT count()
FROM (SELECT number AS k FROM numbers(100)) AS t1
FULL JOIN (SELECT number + 50 AS k FROM numbers(100)) AS t2
ON t1.k = t2.k;

-- Test 5: INNER JOIN that exceeds max_bytes_in_join and must spill.
SELECT 'inner join spill';
SET max_bytes_in_join = 100000;
SELECT count(), sum(t2.v)
FROM (SELECT number AS k FROM numbers(10000)) AS t1
INNER JOIN (SELECT number AS k, number AS v FROM numbers(10000)) AS t2
ON t1.k = t2.k;

-- Test 6: LEFT JOIN that spills.
SELECT 'left join spill';
SELECT count()
FROM (SELECT number AS k FROM numbers(10000)) AS t1
LEFT JOIN (SELECT number + 5000 AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k;

-- Test 7: RIGHT JOIN that spills.
SELECT 'right join spill';
SELECT count()
FROM (SELECT number + 5000 AS k FROM numbers(10000)) AS t1
RIGHT JOIN (SELECT number AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k;

-- Test 8: FULL JOIN that spills.
SELECT 'full join spill';
SELECT count()
FROM (SELECT number AS k FROM numbers(10000)) AS t1
FULL JOIN (SELECT number + 5000 AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k;
SET max_bytes_in_join = 0;

-- ====================================================================
-- Concurrent path: SpillingHashJoin wrapping ConcurrentHashJoin.
-- ====================================================================
SET join_algorithm = 'parallel_hash';
SET max_threads = 4;

-- Test 9: Small INNER JOIN that fits in memory (concurrent, no spill).
SELECT 'concurrent inner join small';
SELECT count(), sum(t2.v)
FROM (SELECT number AS k FROM numbers(100)) AS t1
INNER JOIN (SELECT number AS k, number * 10 AS v FROM numbers(100)) AS t2
ON t1.k = t2.k;

-- Test 10: Small LEFT JOIN that fits in memory (concurrent, no spill).
SELECT 'concurrent left join small';
SELECT count(), sum(if(t2.k = 0 AND t1.k != 0, 0, t2.v))
FROM (SELECT number AS k FROM numbers(100)) AS t1
LEFT JOIN (SELECT number + 50 AS k, number AS v FROM numbers(100)) AS t2
ON t1.k = t2.k;

-- Test 11: INNER JOIN that exceeds max_bytes_in_join and must spill (concurrent).
SELECT 'concurrent inner join spill';
SET max_bytes_in_join = 100000;
SELECT count(), sum(t2.v)
FROM (SELECT number AS k FROM numbers(10000)) AS t1
INNER JOIN (SELECT number AS k, number AS v FROM numbers(10000)) AS t2
ON t1.k = t2.k;

-- Test 12: LEFT JOIN that spills (concurrent).
SELECT 'concurrent left join spill';
SELECT count()
FROM (SELECT number AS k FROM numbers(10000)) AS t1
LEFT JOIN (SELECT number + 5000 AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k;

-- Test 13: RIGHT JOIN that spills (concurrent).
SELECT 'concurrent right join spill';
SELECT count()
FROM (SELECT number + 5000 AS k FROM numbers(10000)) AS t1
RIGHT JOIN (SELECT number AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k;

-- Test 14: FULL JOIN that spills (concurrent).
SELECT 'concurrent full join spill';
SELECT count()
FROM (SELECT number AS k FROM numbers(10000)) AS t1
FULL JOIN (SELECT number + 5000 AS k FROM numbers(10000)) AS t2
ON t1.k = t2.k;
SET max_bytes_in_join = 0;
