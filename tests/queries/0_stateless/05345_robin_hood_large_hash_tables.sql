-- Tags: long, no-tsan
-- no-tsan: the queries run in a single thread, so TSan has nothing to check, and they are too slow under it
-- Large two-level hash tables switch to the Robin Hood mode with a higher load factor.
-- Check that aggregation, `uniqExact` and `JOIN` stay correct with millions of keys.

-- 9 million keys is about 35 thousand keys per bucket, above the switching point of 32768.
-- With a single thread, the per-thread table is the whole table, so it becomes large too.
-- The queries are kept small, because the test is run many times in parallel in the flaky check.
SET max_threads = 1, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0, max_rows_to_read = 0;

SELECT 'UInt64 keys';
SELECT count(), min(c), max(c)
FROM (SELECT intHash64(number) AS k, count() AS c FROM numbers(9000000) GROUP BY k);

SELECT 'Keys with the same low bits';
SELECT count(), min(c), max(c)
FROM (SELECT number * 65536 AS k, count() AS c FROM numbers(9000000) GROUP BY k);

SELECT 'String keys';
SELECT count(), min(c), max(c)
FROM (SELECT toString(number) AS s, count() AS c FROM numbers(9000000) GROUP BY s);

SELECT 'uniqExact';
SELECT uniqExact(number) FROM numbers(9000000);

SELECT 'Merge of two large uniqExact states';
-- The states are read back as two-level sets with about 35 thousand keys per bucket each: together they outnumber
-- the cells of a bucket.
DROP TABLE IF EXISTS t_uniq_exact_states;
CREATE TABLE t_uniq_exact_states (s AggregateFunction(uniqExact, UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_uniq_exact_states SELECT uniqExactState(number) FROM numbers(9000000);
INSERT INTO t_uniq_exact_states SELECT uniqExactState(number + 9000000) FROM numbers(9000000);
SELECT uniqExactMerge(s) FROM t_uniq_exact_states;
DROP TABLE t_uniq_exact_states;

SELECT 'Merge of two-level states';
-- With `prefer_localhost_replica = 1` the single thread aggregates the local shard and does not finish the handshake
-- with the remote one in time, so the remote server closes the connection after `handshake_timeout_milliseconds`.
SELECT count(), sum(c) FROM (SELECT number AS k, count() AS c FROM remote('127.0.0.{1,2}', numbers(9000000)) GROUP BY k)
SETTINGS prefer_localhost_replica = 0;

SELECT 'JOIN';
-- Only `parallel_hash` uses two-level maps, plain `hash` uses one-level maps.
-- The two-level map is shared by all slots, so its buckets are large even with a single thread.
SELECT count(), sum(r) FROM numbers(9000000) AS l
INNER JOIN (SELECT number * 2 AS k, number AS r FROM numbers(9000000)) AS rt ON l.number = rt.k
SETTINGS join_algorithm = 'parallel_hash';
