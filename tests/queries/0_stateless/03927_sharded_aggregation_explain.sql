-- Tags: no-random-merge-tree-settings, no-random-settings
-- EXPLAIN output may differ

SET max_threads = 8;

DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    a String,
    b UInt64,
    u8 UInt8,
    nullable_key Nullable(String)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO test
SELECT
    toString(rand() % 100000) AS a,
    number AS b,
    toUInt8(number % 250) AS u8,
    if(number % 10 = 0, NULL, toString(number % 50000)) AS nullable_key
FROM numbers(300000);

SELECT 'Single String key + sum';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Numeric expression key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT b % 1000 AS k, sum(b) FROM test GROUP BY k
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'No aggregate functions (aggregates_size == 0)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a FROM test GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'count() fast path (is_simple_count)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, count() FROM test GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Multiple aggregate functions (sum, count, max)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b), count(), max(b) FROM test GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Nullable key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT nullable_key, sum(b) FROM test GROUP BY nullable_key
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Nullable key with diverse underlying NULL data';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT nullIf(a, a) AS k, sum(b) FROM test GROUP BY k
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Setting is off: transform is be applied';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 0
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'UInt16 key (key16)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT toUInt16(b % 60000) AS k, sum(b) FROM test GROUP BY k
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'UInt32 key (key32)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT toUInt32(b % 100000) AS k, sum(b) FROM test GROUP BY k
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'UInt64 key (key64)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT b AS k, sum(b) FROM test GROUP BY k
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'FixedString key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT toFixedString(a, 10) AS k, sum(b) FROM test GROUP BY k
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Int16 key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT toInt16(b % 30000) - 15000 AS k, sum(b) FROM test GROUP BY k
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Int32 key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT toInt32(b % 100000) AS k, sum(b) FROM test GROUP BY k
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Int64 key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT toInt64(b) AS k, sum(b) FROM test GROUP BY k
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Float32 key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT toFloat32(b % 1000) AS k, sum(b) FROM test GROUP BY k
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Float64 key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT toFloat64(b % 1000) AS k, sum(b) FROM test GROUP BY k
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'min';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, min(b) FROM test GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'avg';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, avg(b) FROM test GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'any';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, any(b) FROM test GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'uniq';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, uniq(b) FROM test GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'uniqExact';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, uniqExact(b) FROM test GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Multi-argument aggregate (argMin)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, argMin(u8, b) FROM test GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Shared argument across aggregates (sum(b), max(b))';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b), max(b) FROM test GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'WITH TOTALS';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test GROUP BY a WITH TOTALS
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'count(non_nullable_column)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, count(b) FROM test GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Empty table';
DROP TABLE IF EXISTS test_empty;
CREATE TABLE test_empty (a String, b UInt64) ENGINE = MergeTree ORDER BY tuple();
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_empty GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Large hash table (exercises prefetch path)';
DROP TABLE IF EXISTS test_large;
CREATE TABLE test_large (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_large SELECT number AS a, number AS b FROM numbers(5000000);
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_large GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Table Sparse';
DROP TABLE IF EXISTS test_sparse;
CREATE TABLE test_sparse
(
    a String,
    b UInt64,
    u8 UInt8,
    nullable_key Nullable(String)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    serialization_info_version='with_types',
    nullable_serialization_version='allow_sparse',
    ratio_of_defaults_for_sparse_serialization=0.05;

INSERT INTO test_sparse
SELECT
    toString(rand() % 100000) AS a,
    number AS b,
    toUInt8(number % 250) AS u8,
    if(number % 10 = 0, NULL, toString(number % 50000)) AS nullable_key
FROM numbers(300000);

SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT nullable_key, sum(b) FROM test_sparse GROUP BY nullable_key
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Sparse aggregate argument';
DROP TABLE IF EXISTS test_sparse_argument;
CREATE TABLE test_sparse_argument
(
    a String,
    b UInt64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;

INSERT INTO test_sparse_argument VALUES
    ('x', 0), ('y', 1), ('z', 0);

SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_sparse_argument GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Explicit external aggregation settings are ignored on sharded path';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_sparse_argument GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1, max_bytes_before_external_group_by = 1
) WHERE explain LIKE '%ScatterByHashTransform%';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_sparse_argument GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1, max_bytes_ratio_before_external_group_by = 0.1
) WHERE explain LIKE '%ScatterByHashTransform%';
