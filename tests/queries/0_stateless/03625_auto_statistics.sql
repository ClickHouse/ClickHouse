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
    merge_max_block_size = 8192; -- prevent extreme per-block values injected by the test harness from making the merge time out

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
-- Basic auto-add behavior. `basic` accepts numeric, String/FixedString, and
-- Nullable / LowCardinality(Nullable) wrappers, so listing it in
-- `auto_statistics_types` adds it to all columns regardless of nullability.
-- =============================================================================
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS test_basic_auto;
CREATE TABLE test_basic_auto (
    a Nullable(Int64) STATISTICS(tdigest),     -- explicit tdigest, auto adds basic + uniq + minmax
    b Nullable(Float64),                       -- only auto types, gets basic + uniq + minmax
    c Int64,                                   -- non-Nullable numeric, gets basic + uniq + minmax
    d LowCardinality(Nullable(Int64)),         -- LC(Nullable), gets basic + uniq + minmax
    s String                                   -- String, gets basic + uniq (no minmax)
) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS auto_statistics_types = 'minmax, uniq, basic';

INSERT INTO test_basic_auto SELECT
    if(number % 2 = 0, NULL, number),
    if(number % 5 = 0, NULL, toFloat64(number)),
    number,
    if(number % 2 = 0, NULL, number % 100),
    toString(number)
FROM numbers(1000);

SELECT 'Basic auto-added to all suitable columns';
SELECT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_basic_auto' AND statistics != '[]'
ORDER BY column, name;

DROP TABLE test_basic_auto;

-- basic removed from auto_statistics_types → not auto-added
DROP TABLE IF EXISTS test_basic_auto_off;
CREATE TABLE test_basic_auto_off (
    a Nullable(Int64) STATISTICS(tdigest)
) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS auto_statistics_types = 'minmax, uniq';
INSERT INTO test_basic_auto_off SELECT if(number % 2 = 0, NULL, number) FROM numbers(1000);

SELECT 'basic disabled -> not added';
SELECT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_basic_auto_off' AND statistics != '[]'
ORDER BY column, name;

DROP TABLE test_basic_auto_off;
