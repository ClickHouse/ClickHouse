DROP TABLE IF EXISTS test_03168;

CREATE TABLE test_03168(k Int, v Int)
engine = MergeTree()
ORDER BY k
SETTINGS min_bytes_for_full_part_storage=100000000;

-- Create small part so that it uses Packed storage type
INSERT INTO test_03168 select number, number FROM numbers(10);

SELECT name, part_storage_type
FROM system.parts WHERE database = currentDatabase() AND table = 'test_03168' AND level = 0;

SELECT partition, name, column, column_modification_time > now() - INTERVAL 10 MINUTE as ts
FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_03168' AND level = 0
ORDER BY column;

DROP TABLE test_03168;
