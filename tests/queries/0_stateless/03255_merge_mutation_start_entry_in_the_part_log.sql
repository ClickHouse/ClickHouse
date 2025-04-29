DROP TABLE IF EXISTS test;
CREATE TABLE test (x UInt8, y UInt8, z String DEFAULT toString(x)) PARTITION BY x ORDER BY x;
INSERT INTO test (x, y) VALUES (1, 1);
INSERT INTO test (x, y) VALUES (1, 2);
OPTIMIZE TABLE test FINAL;
INSERT INTO test (x, y) VALUES (2, 1);
ALTER TABLE test DROP PARTITION 2;
SET mutations_sync = 1;
ALTER TABLE test UPDATE z = x || y WHERE 1;
SELECT * FROM test ORDER BY ALL;
TRUNCATE TABLE test;
DROP TABLE test SYNC;
SYSTEM FLUSH LOGS;

SELECT event_type, merge_reason, table, part_name, partition_id, partition, rows, merged_from
FROM system.part_log WHERE database = currentDatabase() AND event_type IN ('MergePartsStart', 'MergeParts', 'MutatePartStart', 'MutatePart')
ORDER BY event_time_microseconds FORMAT Vertical;
