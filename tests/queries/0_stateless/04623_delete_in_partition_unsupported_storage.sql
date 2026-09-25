-- Tags: use-rocksdb

-- Storages that go through the `supportsDelete` branch of `InterpreterDeleteQuery`
-- have no MergeTree-style partitions. `DELETE ... IN PARTITION` must be rejected there
-- instead of silently deleting from the whole table.

DROP TABLE IF EXISTS t_delete_in_partition_rocksdb;
CREATE TABLE t_delete_in_partition_rocksdb (key UInt64, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO t_delete_in_partition_rocksdb VALUES (1, 'a'), (2, 'b');

DELETE FROM t_delete_in_partition_rocksdb IN PARTITION 'p1' WHERE key = 1; -- { serverError NOT_IMPLEMENTED }
DELETE FROM t_delete_in_partition_rocksdb IN PARTITION 'p1', 'p2' WHERE key = 1; -- { serverError NOT_IMPLEMENTED }

-- Nothing was deleted by the rejected queries.
SELECT count() FROM t_delete_in_partition_rocksdb;

-- A plain DELETE still works.
DELETE FROM t_delete_in_partition_rocksdb WHERE key = 1;
SELECT count() FROM t_delete_in_partition_rocksdb;

DROP TABLE t_delete_in_partition_rocksdb;
