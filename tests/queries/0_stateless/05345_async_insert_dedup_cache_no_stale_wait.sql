-- Tags: zookeeper

-- An async insert checks its deduplication hashes against the table's deduplication cache. The check waits
-- for a newer cache snapshot only after Keeper found a duplicate that the checked snapshot missed. A wait
-- that times out logs `Read cache with a old version {}`. No insert below has duplicates, so none may wait.

DROP TABLE IF EXISTS t SYNC;
DROP TABLE IF EXISTS src;

CREATE TABLE t (p UInt8, x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1')
PARTITION BY p ORDER BY x
SETTINGS deduplication_hashes_cache_update_wait_ms = 1000;

CREATE TABLE src AS t ENGINE = MergeTree PARTITION BY p ORDER BY x;

SET async_insert = 1, wait_for_async_insert = 1, async_insert_deduplicate = 1, insert_deduplicate = 1;

-- One flush commits three partitions.
INSERT INTO t VALUES (1, 1), (2, 2), (3, 3);

-- REPLACE PARTITION removes the deduplication hashes of the partition and drops the cache snapshot.
INSERT INTO src SETTINGS async_insert = 0 VALUES (1, 10);
ALTER TABLE t REPLACE PARTITION 1 FROM src;
INSERT INTO t VALUES (1, 1);

SELECT p, count() FROM t GROUP BY p ORDER BY p;

SYSTEM FLUSH LOGS text_log;
SELECT count() FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND logger_name = currentDatabase() || '.t (AsyncBlockIDsCache)'
    AND message_format_string = 'Read cache with a old version {}';

DROP TABLE t SYNC;
DROP TABLE src;
