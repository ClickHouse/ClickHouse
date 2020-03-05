DROP TABLE IF EXISTS test_copy_partition_src;
DROP TABLE IF EXISTS test_copy_partition_dest;

CREATE TABLE IF NOT EXISTS test_copy_partition_src (
    pk UInt8,
    val UInt32
) Engine = MergeTree()
  PARTITION BY pk
  ORDER BY (pk, val);

CREATE TABLE IF NOT EXISTS test_copy_partition_dest (
    pk UInt8,
    val UInt32
) Engine = MergeTree()
  PARTITION BY pk
  ORDER BY (pk, val);

INSERT INTO test_copy_partition_src SELECT number % 2, number FROM system.numbers LIMIT 10000000;

SELECT count() FROM test_copy_partition_src;
SELECT count() FROM test_copy_partition_dest;

ALTER TABLE test_copy_partition_dest COPY PARTITION 1 FROM test_copy_partition_src;

SELECT count() FROM test_copy_partition_src;
SELECT count() FROM test_copy_partition_dest;

DROP TABLE test_copy_partition_src;
DROP TABLE test_copy_partition_dest;