-- When the SELECT list of a query over several shards consists of window functions only, the shards
-- have nothing to return: the rows of their part of the result carry no values at all, and a block
-- takes its number of rows from its columns. The rows of every shard but the local one used to be
-- lost, so the window step on the initiator saw fewer rows, or none at all.
--
-- The labels are added outside the distributed subquery on purpose: a constant in the SELECT list of
-- the query over the shards is computed by the shards without the old analyzer, which hides the bug.

SET enable_analyzer = 1;
SET prefer_localhost_replica = 1;

SELECT 'control', count() FROM remote('127.0.0.{1,2}', system.one);
SELECT 'window', c FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', system.one));
SELECT 'window with a column', d, c FROM (SELECT dummy AS d, count() OVER () AS c FROM remote('127.0.0.{1,2}', system.one)) ORDER BY d, c;
SELECT 'every shard is remote', c FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', system.one)) SETTINGS prefer_localhost_replica = 0;
SELECT 'three shards', c FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2,3}', system.one));
SELECT 'several rows per shard', count(), sum(r) FROM (SELECT row_number() OVER () AS r FROM remote('127.0.0.{1,2}', numbers(3)));
SELECT 'rank', count(), max(r) FROM (SELECT rank() OVER () AS r FROM remote('127.0.0.{1,2}', system.one));
SELECT 'cluster', c FROM (SELECT count() OVER () AS c FROM cluster('test_cluster_two_shards', system.one));
SELECT 'filter above the window', count() FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', numbers(4))) WHERE c = 8;

-- The row count has to survive every shape of the receiving side.
SELECT 'without block marshalling', c FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', system.one)) SETTINGS prefer_localhost_replica = 0, enable_parallel_blocks_marshalling = 0;
SELECT 'without compression', c FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', system.one)) SETTINGS prefer_localhost_replica = 0, network_compression_method = 'NONE';
SELECT 'synchronous socket', c FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', system.one)) SETTINGS prefer_localhost_replica = 0, use_hedged_requests = 0, async_socket_for_remote = 0;
SELECT 'hedged requests', c FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', system.one)) SETTINGS prefer_localhost_replica = 0, use_hedged_requests = 1;

SELECT 'old analyzer', c FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', system.one)) SETTINGS enable_analyzer = 0;
SELECT 'old analyzer, every shard is remote', c FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', system.one)) SETTINGS enable_analyzer = 0, prefer_localhost_replica = 0;
