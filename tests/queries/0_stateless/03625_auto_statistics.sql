DROP TABLE IF EXISTS test_table;
SET allow_statistics = 1;
SET materialize_statistics_on_insert = 1;

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
    auto_statistics_types = 'uniq,minmax',
    index_granularity = 8192; -- avoid tiny index_granularity that makes OPTIMIZE FINAL time out

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

-- =============================================================================
-- NullCount auto-add behavior for Nullable columns
-- =============================================================================
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS test_nullcount_auto;
CREATE TABLE test_nullcount_auto (
    a Nullable(Int64) STATISTICS(tdigest),     -- explicit tdigest, auto adds nullcount
    b Nullable(Float64),                       -- only auto types, gets nullcount
    c Int64,                                   -- non-Nullable, no nullcount
    d LowCardinality(Nullable(Int64))          -- LC(Nullable), gets nullcount
) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS auto_statistics_types = 'minmax, uniq, nullcount';

INSERT INTO test_nullcount_auto SELECT
    if(number % 2 = 0, NULL, number),
    if(number % 5 = 0, NULL, toFloat64(number)),
    number,
    if(number % 2 = 0, NULL, number % 100)
FROM numbers(1000);

SELECT 'Nullable columns auto-get nullcount; non-Nullable does not';
SELECT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_auto' AND statistics != '[]'
ORDER BY column, name;

DROP TABLE test_nullcount_auto;

-- nullcount removed from auto_statistics_types → not auto-added even on Nullable
DROP TABLE IF EXISTS test_nullcount_auto_off;
CREATE TABLE test_nullcount_auto_off (
    a Nullable(Int64) STATISTICS(tdigest)
) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS auto_statistics_types = 'minmax, uniq';
INSERT INTO test_nullcount_auto_off SELECT if(number % 2 = 0, NULL, number) FROM numbers(1000);

SELECT 'nullcount disabled → not added';
SELECT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_auto_off' AND statistics != '[]'
ORDER BY column, name;

DROP TABLE test_nullcount_auto_off;
