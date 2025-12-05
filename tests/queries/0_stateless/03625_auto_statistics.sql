DROP TABLE IF EXISTS test_table;
SET allow_experimental_statistics = 1;

CREATE TABLE test_table
(
    id UInt64,
    v1 String STATISTICS(uniq),
    v2 UInt64 STATISTICS(tdigest),
    v3 String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    auto_statistics_types = 'uniq,minmax';

SYSTEM STOP MERGES test_table;

INSERT INTO test_table SELECT number, if (rand() % 100 = 0, 'foo', ''), rand() % 2, rand() % 2 FROM numbers(100000);
INSERT INTO test_table SELECT number, if (rand() % 100 = 0, 'bar', ''), rand() % 2 + 5, rand() % 2 + 5 FROM numbers(100000);

SELECT name, column, type, statistics, estimates.cardinality, estimates.min, estimates.max
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_table' AND active
ORDER BY name, column;

SELECT count() FROM test_table WHERE NOT ignore(*);
SELECT uniqExact(v1), uniqExact(v2), uniqExact(v3) FROM test_table WHERE NOT ignore(*);

SYSTEM START MERGES test_table;
OPTIMIZE TABLE test_table FINAL;

SELECT name, column, type, statistics, estimates.cardinality, estimates.min, estimates.max
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_table' AND active
ORDER BY name, column;

SELECT count() FROM test_table WHERE NOT ignore(*);
SELECT uniqExact(v1), uniqExact(v2), uniqExact(v3) FROM test_table WHERE NOT ignore(*);

DETACH TABLE test_table;
ATTACH TABLE test_table;

SELECT name, column, type, statistics, estimates.cardinality, estimates.min, estimates.max
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_table' AND active
ORDER BY name, column;

SELECT count() FROM test_table WHERE NOT ignore(*);
SELECT uniqExact(v1), uniqExact(v2), uniqExact(v3) FROM test_table WHERE NOT ignore(*);

DROP TABLE IF EXISTS test_table;
