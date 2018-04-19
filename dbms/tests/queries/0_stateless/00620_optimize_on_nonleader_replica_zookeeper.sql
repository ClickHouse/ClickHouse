DROP TABLE IF EXISTS test.clear_column1;
DROP TABLE IF EXISTS test.clear_column2;
CREATE TABLE test.clear_column1 (p Int64, i Int64, v UInt64) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/test/clear_column', '1', v) PARTITION BY p ORDER BY i;
CREATE TABLE test.clear_column2 (p Int64, i Int64, v UInt64) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/test/clear_column', '2', v) PARTITION BY p ORDER BY i;

INSERT INTO test.clear_column1 VALUES (0, 1, 0);
INSERT INTO test.clear_column1 VALUES (0, 1, 1);

OPTIMIZE TABLE test.clear_column1;
OPTIMIZE TABLE test.clear_column2;
SELECT * FROM test.clear_column1;

RENAME TABLE test.clear_column2 TO test.clear_column3;

INSERT INTO test.clear_column1 VALUES (0, 1, 2);
OPTIMIZE TABLE test.clear_column3;
SELECT * FROM test.clear_column1;

DROP TABLE IF EXISTS test.clear_column1;
DROP TABLE IF EXISTS test.clear_column2;