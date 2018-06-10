DROP TABLE IF EXISTS test.table_old;
DROP TABLE IF EXISTS test.table_new;

CREATE TABLE test.table_old (k UInt64, d Array(String)) ENGINE = ReplicatedMergeTree('/clickhouse/test/tables/checksums_test', 'old') ORDER BY k SETTINGS use_minimalistic_checksums_in_zookeeper=0;
CREATE TABLE test.table_new (k UInt64, d Array(String)) ENGINE = ReplicatedMergeTree('/clickhouse/test/tables/checksums_test', 'new') ORDER BY k SETTINGS use_minimalistic_checksums_in_zookeeper=1;

SET insert_quorum=2;
INSERT INTO test.table_old VALUES (0, []);
SELECT value LIKE '%checksums format version: 4%' FROM system.zookeeper WHERE path='/clickhouse/test/tables/checksums_test/replicas/old/parts/all_0_0_0' AND name = 'checksums';

INSERT INTO test.table_new VALUES (1, []);
SELECT value LIKE '%checksums format version: 5%' FROM system.zookeeper WHERE path='/clickhouse/test/tables/checksums_test/replicas/new/parts/all_1_1_0' AND name = 'checksums';

OPTIMIZE TABLE test.table_old;
SELECT * FROM test.table_old ORDER BY k;
SELECT * FROM test.table_new ORDER BY k;

SELECT 'DETACH';
DETACH TABLE test.table_old;
ATTACH TABLE test.table_old (k UInt64, d Array(String)) ENGINE = ReplicatedMergeTree('/clickhouse/test/tables/checksums_test', 'old') ORDER BY k SETTINGS use_minimalistic_checksums_in_zookeeper=1;
SELECT * FROM test.table_old ORDER BY k;

DROP TABLE IF EXISTS test.table_old;
DROP TABLE IF EXISTS test.table_new;