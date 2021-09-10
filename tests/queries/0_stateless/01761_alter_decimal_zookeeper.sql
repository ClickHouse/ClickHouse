DROP TABLE IF EXISTS test_alter_decimal;

CREATE TABLE test_alter_decimal
(n UInt64, d Decimal(15, 8))
ENGINE = ReplicatedMergeTree('/clickhouse/01761_alter_decimal_zookeeper', 'r1')
ORDER BY tuple();

INSERT INTO test_alter_decimal VALUES (1, toDecimal32(5, 5));

INSERT INTO test_alter_decimal VALUES (2, toDecimal32(6, 6));

SELECT * FROM test_alter_decimal ORDER BY n;

ALTER TABLE test_alter_decimal MODIFY COLUMN d Decimal(18, 8);

SHOW CREATE TABLE test_alter_decimal;

SELECT * FROM test_alter_decimal ORDER BY n;

DETACH TABLE test_alter_decimal;
ATTACH TABLE test_alter_decimal;

SHOW CREATE TABLE test_alter_decimal;

INSERT INTO test_alter_decimal VALUES (3, toDecimal32(7, 7));

OPTIMIZE TABLE test_alter_decimal FINAL;

SELECT * FROM test_alter_decimal ORDER BY n;

DROP TABLE IF EXISTS test_alter_decimal;
