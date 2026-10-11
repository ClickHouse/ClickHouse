-- A partial dedup retry must not write a row the first split already expired.

DROP TABLE IF EXISTS t_ttl_dedup_retry;

CREATE TABLE t_ttl_dedup_retry (d DateTime, x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_ttl_dedup_retry', 'r1')
ORDER BY x
TTL d + INTERVAL 1 DAY
SETTINGS apply_ttl_delete_on_insert = 1;

SYSTEM STOP MERGES t_ttl_dedup_retry;

INSERT INTO t_ttl_dedup_retry SETTINGS insert_deduplication_token = 'dup' VALUES ('2100-01-01 00:00:00', 1);

SET async_insert = 1;
SET wait_for_async_insert = 0;
SET async_insert_deduplicate = 1;
SET async_insert_use_adaptive_busy_timeout = 0;
SET async_insert_busy_timeout_min_ms = 1000;
SET async_insert_busy_timeout_max_ms = 5000;

INSERT INTO t_ttl_dedup_retry SETTINGS insert_deduplication_token = 'keep' VALUES ('2000-01-01 00:00:00', 2), ('2100-01-01 00:00:00', 3);
INSERT INTO t_ttl_dedup_retry SETTINGS insert_deduplication_token = 'dup' VALUES ('2100-01-01 00:00:00', 1);
SYSTEM FLUSH ASYNC INSERT QUEUE t_ttl_dedup_retry;

SELECT x FROM t_ttl_dedup_retry ORDER BY x;

DROP TABLE t_ttl_dedup_retry;
