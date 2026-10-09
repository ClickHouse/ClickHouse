-- Large two-level hash tables switch to the Robin Hood mode with a higher load factor.
-- Check that aggregation, `uniqExact` and `JOIN` stay correct with millions of keys.

-- 10 million keys is about 39 thousand keys per bucket, above the switching point of 32768.
-- With few threads, the per-thread tables also become large, not only the merged one.
SET max_threads = 2, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT 'UInt64 keys';
SELECT count(), min(c), max(c)
FROM (SELECT intHash64(number % 10000000) AS k, count() AS c FROM numbers_mt(20000000) GROUP BY k);

SELECT 'Keys with the same low bits';
SELECT count(), min(c), max(c)
FROM (SELECT (number % 10000000) * 65536 AS k, count() AS c FROM numbers_mt(20000000) GROUP BY k);

SELECT 'String keys';
SELECT count(), min(c), max(c)
FROM (SELECT number % 10000000 AS k, toString(k) AS s, count() AS c FROM numbers_mt(20000000) GROUP BY k, s);

SELECT 'Without aggregate functions';
SELECT count() FROM (SELECT number % 10000000 AS k FROM numbers_mt(20000000) GROUP BY k);

SELECT 'uniqExact';
SELECT uniqExact(number % 10000000) FROM numbers_mt(20000000);

SELECT 'Merge of two-level states';
SELECT count(), sum(c) FROM (SELECT number % 10000000 AS k, count() AS c FROM remote('127.0.0.{1,2}', numbers(10000000)) GROUP BY k);

SELECT 'JOIN';
SELECT count(), sum(r) FROM numbers(20000000) AS l
INNER JOIN (SELECT number * 2 AS k, number AS r FROM numbers(10000000)) AS rt ON l.number = rt.k
SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(r) FROM numbers(20000000) AS l
INNER JOIN (SELECT number * 2 AS k, number AS r FROM numbers(10000000)) AS rt ON l.number = rt.k
SETTINGS join_algorithm = 'hash';
