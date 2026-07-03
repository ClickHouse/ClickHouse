CREATE DATABASE test_db;
CREATE TABLE test_db.test_table (
  id UUID,
  value UInt16
) ENGINE = MergeTree()
ORDER BY id;
INSERT INTO test_db.test_table
SELECT generateUUIDv4(), 4::UInt16
FROM numbers(50);
