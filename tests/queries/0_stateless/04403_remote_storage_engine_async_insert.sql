-- A persistent `Remote` engine builds an ad-hoc owned cluster, just like the `remote()` table
-- function, but unlike the table function it has a data path. It must therefore be able to use the
-- asynchronous `Distributed` insert queue (and honor `distributed_foreground_insert` and the related
-- queue settings) instead of always inserting synchronously. This is a regression test for that.

DROP TABLE IF EXISTS target_04403;
DROP TABLE IF EXISTS remote_04403;

CREATE TABLE target_04403 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE remote_04403 (x UInt64) ENGINE = Remote('127.0.0.1', currentDatabase(), target_04403);

-- `prefer_localhost_replica = 0` forces even the local shard through the distribution queue.
SYSTEM STOP DISTRIBUTED SENDS remote_04403;
INSERT INTO remote_04403 SETTINGS distributed_foreground_insert = 0, prefer_localhost_replica = 0 VALUES (1) (2) (3);

-- With foreground insert disabled the rows are spooled to the queue, not sent synchronously:
-- the queue has pending files and the target is still empty.
SELECT 'queued', data_files > 0 FROM system.distribution_queue WHERE database = currentDatabase() AND table = 'remote_04403';
SELECT 'before_flush', count() FROM target_04403;

SYSTEM FLUSH DISTRIBUTED remote_04403;
SELECT 'after_flush', count() FROM target_04403;

DROP TABLE remote_04403;
DROP TABLE target_04403;
