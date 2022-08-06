DROP TABLE IF EXISTS test;
CREATE TABLE test (x UInt64) ENGINE = MergeTree ORDER BY x;

SET max_block_size = 1;
INSERT INTO test SELECT * FROM system.numbers LIMIT 1000;
OPTIMIZE TABLE test FINAL;

SYSTEM FLUSH LOGS;

-- ProfileEvents field exist and contains something plausible:
SELECT count() > 0 FROM system.part_log WHERE ProfileEvents['MergedRows'] = 1000 AND table = 'test' AND database = currentDatabase() AND event_time >= now() - 600;

DROP TABLE test;
