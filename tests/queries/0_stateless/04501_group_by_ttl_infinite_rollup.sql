-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105647

DROP TABLE IF EXISTS test_ttl_group_by SYNC;

CREATE TABLE test_ttl_group_by (key UInt32, ts DateTime, value UInt32)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY key
TTL ts + INTERVAL 3 MONTH GROUP BY key SET value = sum(value),
    ts + INTERVAL 50 YEAR DELETE
SETTINGS merge_with_ttl_timeout = 0;

INSERT INTO test_ttl_group_by VALUES (1, '2020-01-01 00:00:00', 1), (1, '2021-01-01 00:00:00', 2), (1, '2020-01-01 00:00:00', 1), (1, '2021-01-01 00:00:00', 2);

OPTIMIZE TABLE test_ttl_group_by FINAL;

SELECT key, value FROM test_ttl_group_by ORDER BY ALL;

-- Give the background merge selector time to reschedule the rollup (it must not).
SELECT sleep(3) FORMAT Null;
SELECT sleep(3) FORMAT Null;

-- The partition holds the inserted part, the rolled up part and at most one part
-- of a background rollup that could run before OPTIMIZE. Any rescheduled rollup adds more.
SELECT count() <= 3 FROM system.parts WHERE database = currentDatabase() AND table = 'test_ttl_group_by' AND partition_id = '202001';

DROP TABLE test_ttl_group_by;
