-- Tags: memory-engine
SET allow_experimental_dynamic_type=1;

DROP TABLE IF EXISTS test_dynamic_compressed;
CREATE TABLE test_dynamic_compressed (d Dynamic) ENGINE=Memory SETTINGS compress=1;
INSERT INTO test_dynamic_compressed SELECT number FROM numbers(5);
ALTER TABLE test_dynamic_compressed MODIFY COLUMN d Dynamic(max_types=0);
SELECT d.UInt64 FROM test_dynamic_compressed ORDER BY d.UInt64;
DROP TABLE test_dynamic_compressed;
