DROP TABLE IF EXISTS test.clear_column;
CREATE TABLE test.clear_column (d Date, num Int64, str String) ENGINE = MergeTree(d, d, 8192);

INSERT INTO test.clear_column VALUES ('2016-12-12', 1, 'a'), ('2016-11-12', 2, 'b');

SELECT num, str FROM test.clear_column ORDER BY num;
ALTER TABLE test.clear_column CLEAR COLUMN num IN PARTITION '201612';
SELECT num, str FROM test.clear_column ORDER BY num;

DROP TABLE test.clear_column;

-- Replicated case

DROP TABLE IF EXISTS test.clear_column1;
DROP TABLE IF EXISTS test.clear_column2;
CREATE TABLE test.clear_column1 (d Date, i Int64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/clear_column', '1', d, d, 8192);
CREATE TABLE test.clear_column2 (d Date, i Int64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/clear_column', '2', d, d, 8192);

INSERT INTO test.clear_column1 (d) VALUES ('2000-01-01'), ('2000-02-01');

SET replication_alter_partitions_sync=2;
ALTER TABLE test.clear_column1 ADD COLUMN s String;
ALTER TABLE test.clear_column1 CLEAR COLUMN s IN PARTITION '200001';

INSERT INTO test.clear_column1 VALUES ('2000-01-01', 1, 'a'), ('2000-02-01', 2, 'b');
INSERT INTO test.clear_column1 VALUES ('2000-01-01', 3, 'c'), ('2000-02-01', 4, 'd');

SELECT 'all';
SELECT * FROM test.clear_column1 ORDER BY d, i, s;

SELECT 'w/o i 1';
ALTER TABLE test.clear_column1 CLEAR COLUMN i IN PARTITION '200001';
SELECT * FROM test.clear_column2 ORDER BY d, i, s;

SELECT 'w/o is 1';
ALTER TABLE test.clear_column1 CLEAR COLUMN s IN PARTITION '200001';
SELECT * FROM test.clear_column2 ORDER BY d, i, s;

SELECT 'w/o is 12';
ALTER TABLE test.clear_column1 CLEAR COLUMN i IN PARTITION '200002';
ALTER TABLE test.clear_column1 CLEAR COLUMN s IN PARTITION '200002';
SELECT DISTINCT * FROM test.clear_column2 ORDER BY d, i, s;
SELECT DISTINCT * FROM test.clear_column2 ORDER BY d, i, s;

SELECT 'sizes';
SELECT sum(data_uncompressed_bytes) FROM system.columns WHERE database='test' AND table LIKE 'clear_column_' AND (name = 'i' OR name = 's') GROUP BY table;

-- double call should be OK
ALTER TABLE test.clear_column1 CLEAR COLUMN s IN PARTITION '200001';
ALTER TABLE test.clear_column1 CLEAR COLUMN s IN PARTITION '200002';

-- clear column in empty partition should be Ok
ALTER TABLE test.clear_column1 CLEAR COLUMN s IN PARTITION '200012', CLEAR COLUMN i IN PARTITION '200012';
-- Drop empty partition also Ok
ALTER TABLE test.clear_column1 DROP PARTITION '200012', DROP PARTITION '200011';


-- check optimize for non-leader replica (it is not related with CLEAR COLUMN)
OPTIMIZE TABLE test.clear_column1;
OPTIMIZE TABLE test.clear_column2;

DROP TABLE IF EXISTS test.clear_column1;
DROP TABLE IF EXISTS test.clear_column2;
