DROP TABLE IF EXISTS test_parts_files;

CREATE TABLE test_parts_files (x UInt64) ENGINE = MergeTree ORDER BY x;

INSERT INTO test_parts_files VALUES (1), (2), (3);

SELECT 'files column exists and is positive';
SELECT files > 0 FROM system.parts WHERE database = currentDatabase() AND table = 'test_parts_files' AND active;

SELECT 'files column type is UInt64';
SELECT toTypeName(files) FROM system.parts WHERE database = currentDatabase() AND table = 'test_parts_files' AND active LIMIT 1;

DROP TABLE test_parts_files;
