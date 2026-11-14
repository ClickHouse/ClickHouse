SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt8,
    value Nullable(Decimal(38, 2))
) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_table VALUES (1, '22.5'), (2, Null);

SELECT id IN toDecimal64(257, NULL) FROM test_table;

DROP TABLE test_table;
