DROP TABLE IF EXISTS test.partitions;
CREATE TABLE test.partitions (x UInt64) ENGINE = MergeTree ORDER BY x PARTITION BY x;

INSERT INTO test.partitions SELECT * FROM system.numbers LIMIT 100;
SELECT count() FROM system.parts WHERE database = 'test' AND table = 'partitions';
INSERT INTO test.partitions SELECT * FROM system.numbers LIMIT 100;
SELECT count() FROM system.parts WHERE database = 'test' AND table = 'partitions';

SET max_partitions_per_insert_block = 1;

INSERT INTO test.partitions SELECT * FROM system.numbers LIMIT 1;
INSERT INTO test.partitions SELECT * FROM system.numbers LIMIT 2; -- { serverError 252 }

DROP TABLE test.partitions;
