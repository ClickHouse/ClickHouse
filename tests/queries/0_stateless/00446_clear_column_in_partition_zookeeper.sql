SELECT '===Ordinary case===';

SET replication_alter_partitions_sync = 2;

DROP TABLE IF EXISTS clear_column;
CREATE TABLE clear_column (d Date, num Int64, str String) ENGINE = MergeTree(d, d, 8192);

INSERT INTO clear_column VALUES ('2016-12-12', 1, 'a'), ('2016-11-12', 2, 'b');

SELECT data_uncompressed_bytes FROM system.columns WHERE (database = currentDatabase()) AND (table = 'clear_column') AND (name = 'num');

SELECT num, str FROM clear_column ORDER BY num;
ALTER TABLE clear_column CLEAR COLUMN num IN PARTITION '201612';
SELECT num, str FROM clear_column ORDER BY num;

SELECT data_uncompressed_bytes FROM system.columns WHERE (database = currentDatabase()) AND (table = 'clear_column') AND (name = 'num');
ALTER TABLE clear_column CLEAR COLUMN num IN PARTITION '201611';
SELECT data_compressed_bytes, data_uncompressed_bytes FROM system.columns WHERE (database = currentDatabase()) AND (table = 'clear_column') AND (name = 'num');

DROP TABLE clear_column;

SELECT '===Replicated case===';

DROP TABLE IF EXISTS clear_column1 NO DELAY;
DROP TABLE IF EXISTS clear_column2 NO DELAY;
SELECT sleep(1) FORMAT Null;
CREATE TABLE clear_column1 (d Date, i Int64) ENGINE = ReplicatedMergeTree('/clickhouse/test/tables/clear_column', '1', d, d, 8192);
CREATE TABLE clear_column2 (d Date, i Int64) ENGINE = ReplicatedMergeTree('/clickhouse/test/tables/clear_column', '2', d, d, 8192);

INSERT INTO clear_column1 (d) VALUES ('2000-01-01'), ('2000-02-01');
SYSTEM SYNC REPLICA clear_column2;

SET replication_alter_partitions_sync=2;
ALTER TABLE clear_column1 ADD COLUMN s String;
ALTER TABLE clear_column1 CLEAR COLUMN s IN PARTITION '200001';

INSERT INTO clear_column1 VALUES ('2000-01-01', 1, 'a'), ('2000-02-01', 2, 'b');
INSERT INTO clear_column1 VALUES ('2000-01-01', 3, 'c'), ('2000-02-01', 4, 'd');
SYSTEM SYNC REPLICA clear_column2;

SELECT 'all';
SELECT * FROM clear_column2 ORDER BY d, i, s;

SELECT 'w/o i 1';
ALTER TABLE clear_column1 CLEAR COLUMN i IN PARTITION '200001';
SELECT * FROM clear_column2 ORDER BY d, i, s;

SELECT 'w/o is 1';
ALTER TABLE clear_column1 CLEAR COLUMN s IN PARTITION '200001';
SELECT * FROM clear_column2 ORDER BY d, i, s;

SELECT 'w/o is 12';
ALTER TABLE clear_column1 CLEAR COLUMN i IN PARTITION '200002';
ALTER TABLE clear_column1 CLEAR COLUMN s IN PARTITION '200002';
SELECT DISTINCT * FROM clear_column2 ORDER BY d, i, s;
SELECT DISTINCT * FROM clear_column2 ORDER BY d, i, s;

SELECT 'sizes';
SELECT sum(data_uncompressed_bytes) FROM system.columns WHERE database=currentDatabase() AND table LIKE 'clear_column_' AND (name = 'i' OR name = 's') GROUP BY table;

-- double call should be OK
ALTER TABLE clear_column1 CLEAR COLUMN s IN PARTITION '200001';
ALTER TABLE clear_column1 CLEAR COLUMN s IN PARTITION '200002';

SET optimize_throw_if_noop = 1;
OPTIMIZE TABLE clear_column1 PARTITION '200001';
OPTIMIZE TABLE clear_column1 PARTITION '200002';


-- clear column in empty partition should be Ok
ALTER TABLE clear_column1 CLEAR COLUMN s IN PARTITION '200012', CLEAR COLUMN i IN PARTITION '200012';
-- Drop empty partition also Ok
ALTER TABLE clear_column1 DROP PARTITION '200012', DROP PARTITION '200011';
