DROP TABLE IF EXISTS test;
CREATE TABLE test (x UInt8) ORDER BY x;
INSERT INTO test VALUES (1);
INSERT INTO test VALUES (2);
OPTIMIZE TABLE test FINAL;
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['Merge'], ProfileEvents['MergeSourceParts'], ProfileEvents['MergedRows'], ProfileEvents['MergedColumns'] FROM system.part_log WHERE database = currentDatabase() AND table = 'test' AND event_type = 'MergeParts';
DROP TABLE test;
