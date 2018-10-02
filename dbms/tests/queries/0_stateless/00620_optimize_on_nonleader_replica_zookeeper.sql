DROP TABLE IF EXISTS test.rename1;
DROP TABLE IF EXISTS test.rename2;
DROP TABLE IF EXISTS test.rename3;
CREATE TABLE test.rename1 (p Int64, i Int64, v UInt64) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/test/tables/rename', '1', v) PARTITION BY p ORDER BY i;
CREATE TABLE test.rename2 (p Int64, i Int64, v UInt64) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/test/tables/rename', '2', v) PARTITION BY p ORDER BY i;

INSERT INTO test.rename1 VALUES (0, 1, 0);
INSERT INTO test.rename1 VALUES (0, 1, 1);

OPTIMIZE TABLE test.rename1;
OPTIMIZE TABLE test.rename2;
SELECT * FROM test.rename1;

RENAME TABLE test.rename2 TO test.rename3;

INSERT INTO test.rename1 VALUES (0, 1, 2);
OPTIMIZE TABLE test.rename3;
SELECT * FROM test.rename1;

DROP TABLE IF EXISTS test.rename1;
DROP TABLE IF EXISTS test.rename2;
DROP TABLE IF EXISTS test.rename3;
