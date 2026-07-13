SET allow_suspicious_low_cardinality_types = 1;

SELECT 'Test fixture for issue: https://github.com/ClickHouse/ClickHouse/issues/72265';

DROP TABLE IF EXISTS test_table_72265_1;
CREATE TABLE test_table_72265_1 
(
    `a` LowCardinality(Nullable(Int64)), 
    `b` UInt64
) 
ENGINE = MergeTree 
PARTITION BY a % 2 
ORDER BY a 
SETTINGS 
    allow_nullable_key = 1, 
    index_granularity = 64, 
    index_granularity_bytes = '10M', 
    min_bytes_for_wide_part = 0;

INSERT INTO test_table_72265_1 SELECT number, number FROM numbers(10000);
SELECT count() FROM test_table_72265_1 WHERE (a > 100) AND ((a % 2) = toUInt128(0));

DROP TABLE IF EXISTS test_table_72265_2;
CREATE TABLE test_table_72265_2
(
    `part` LowCardinality(Nullable(Int64))
)
ENGINE = MergeTree
PARTITION BY part
ORDER BY part
SETTINGS allow_nullable_key = 1;
INSERT INTO test_table_72265_2 (part) FORMAT Values (1);
SELECT * FROM test_table_72265_2 PREWHERE part = toUInt128(1);
