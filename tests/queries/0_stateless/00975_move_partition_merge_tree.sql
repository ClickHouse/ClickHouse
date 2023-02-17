DROP TABLE IF EXISTS test_move_partition_src;
DROP TABLE IF EXISTS test_move_partition_dest;

CREATE TABLE IF NOT EXISTS test_move_partition_src (
    pk UInt8,
    val UInt32
) Engine = MergeTree()
  PARTITION BY pk
  ORDER BY (pk, val);

CREATE TABLE IF NOT EXISTS test_move_partition_dest (
    pk UInt8,
    val UInt32
) Engine = MergeTree()
  PARTITION BY pk
  ORDER BY (pk, val);

INSERT INTO test_move_partition_src SELECT number % 2, number FROM system.numbers LIMIT 10000000;

SELECT count() FROM test_move_partition_src;
SELECT count() FROM test_move_partition_dest;

ALTER TABLE test_move_partition_src MOVE PARTITION 1 TO TABLE test_move_partition_dest;

SELECT count() FROM test_move_partition_src;
SELECT count() FROM test_move_partition_dest;

DROP TABLE test_move_partition_src;
DROP TABLE test_move_partition_dest;
