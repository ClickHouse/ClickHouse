-- Tags: zookeeper

-- An async insert checks its deduplication hashes against the table's deduplication cache. The check waits
-- for a newer cache snapshot only after Keeper found a duplicate that the checked snapshot missed. A wait
-- that times out logs `Read cache with a old version {}`. No insert below has duplicates, so none may wait.

DROP TABLE IF EXISTS t SYNC;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS start_time;

CREATE TABLE t (p UInt8, x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1')
PARTITION BY p ORDER BY x
SETTINGS deduplication_hashes_cache_update_wait_ms = 1000;

CREATE TABLE src AS t ENGINE = MergeTree PARTITION BY p ORDER BY x;

SET async_insert = 1, wait_for_async_insert = 1, async_insert_deduplicate = 1, insert_deduplicate = 1;

-- The first check waits for the initial cache load.
INSERT INTO t VALUES (0, 0);

CREATE TABLE start_time (value DateTime64(6)) ENGINE = Memory;
INSERT INTO start_time SETTINGS async_insert = 0 SELECT now64(6);

-- One flush commits three partitions.
INSERT INTO t VALUES (1, 1), (2, 2), (3, 3);

-- Keeper faults retry the same commit attempt.
INSERT INTO t SETTINGS insert_keeper_fault_injection_probability = 0.3, insert_keeper_max_retries = 100, insert_keeper_retry_max_backoff_ms = 10 VALUES (4, 1);
INSERT INTO t SETTINGS insert_keeper_fault_injection_probability = 0.3, insert_keeper_max_retries = 100, insert_keeper_retry_max_backoff_ms = 10 VALUES (4, 2);
INSERT INTO t SETTINGS insert_keeper_fault_injection_probability = 0.3, insert_keeper_max_retries = 100, insert_keeper_retry_max_backoff_ms = 10 VALUES (4, 3);
INSERT INTO t SETTINGS insert_keeper_fault_injection_probability = 0.3, insert_keeper_max_retries = 100, insert_keeper_retry_max_backoff_ms = 10 VALUES (4, 4);
INSERT INTO t SETTINGS insert_keeper_fault_injection_probability = 0.3, insert_keeper_max_retries = 100, insert_keeper_retry_max_backoff_ms = 10 VALUES (4, 5);

-- REPLACE PARTITION removes the deduplication hashes of the partition and drops the cache snapshot.
INSERT INTO src SETTINGS async_insert = 0 VALUES (1, 10);
ALTER TABLE t REPLACE PARTITION 1 FROM src;
INSERT INTO t VALUES (1, 1);

SELECT p, count() FROM t GROUP BY p ORDER BY p;

SYSTEM FLUSH LOGS text_log;
SELECT count() FROM system.text_log
WHERE event_date >= yesterday() AND event_time_microseconds > (SELECT value FROM start_time)
    AND logger_name = currentDatabase() || '.t (AsyncBlockIDsCache)'
    AND message_format_string = 'Read cache with a old version {}';

DROP TABLE t SYNC;
DROP TABLE src;
DROP TABLE start_time;
