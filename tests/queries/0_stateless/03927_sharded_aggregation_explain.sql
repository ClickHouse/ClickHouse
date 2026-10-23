-- Tags: no-random-merge-tree-settings, no-random-settings, no-parallel-replicas
-- EXPLAIN output may differ

SET max_rows_to_group_by = 0;
SET max_threads = 8;

DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    a String,
    b UInt64,
    u8 UInt8,
    nullable_key Nullable(String),
    arr Array(UInt64),
    flag UInt8
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO test
SELECT
    toString(rand() % 100000) AS a,
    number AS b,
    toUInt8(number % 250) AS u8,
    if(number % 10 = 0, NULL, toString(number % 50000)) AS nullable_key,
    [number % 3, number % 7, number % 11] AS arr,
    toUInt8(number % 2) AS flag
FROM numbers(300000);

SELECT 'Single String key + sum';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Numeric expression key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT b % 1000 AS k, sum(b) FROM test GROUP BY k
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'No aggregate functions (aggregates_size == 0)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'count() fast path (is_simple_count)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, count() FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'count() with UInt64 key (low cardinality, consecutive keys cache)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT toUInt64(b % 100) AS k, count() FROM test GROUP BY k
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Multiple aggregate functions (sum, count, max)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b), count(), max(b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Nullable key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT nullable_key, sum(b) FROM test GROUP BY nullable_key
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Nullable key with diverse underlying NULL data';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT nullIf(a, a) AS k, sum(b) FROM test GROUP BY k
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Setting is off: transform is not applied';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 0
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'UInt16 key (key16)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT toUInt16(b % 60000) AS k, sum(b) FROM test GROUP BY k
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'UInt32 key (key32)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT toUInt32(b % 100000) AS k, sum(b) FROM test GROUP BY k
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'UInt64 key (key64)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT b AS k, sum(b) FROM test GROUP BY k
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'FixedString key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT toFixedString(a, 10) AS k, sum(b) FROM test GROUP BY k
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Int16 key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT toInt16(b % 30000) - 15000 AS k, sum(b) FROM test GROUP BY k
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Int32 key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT toInt32(b % 100000) AS k, sum(b) FROM test GROUP BY k
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Int64 key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT toInt64(b) AS k, sum(b) FROM test GROUP BY k
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Float32 key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT toFloat32(b % 1000) AS k, sum(b) FROM test GROUP BY k
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Float64 key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT toFloat64(b % 1000) AS k, sum(b) FROM test GROUP BY k
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'min';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, min(b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'avg';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, avg(b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'any';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, any(b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'uniq';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, uniq(b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'uniqExact';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, uniqExact(b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Multi-argument aggregate (argMin)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, argMin(u8, b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Shared argument across aggregates (sum(b), max(b))';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b), max(b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'WITH TOTALS';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test GROUP BY a WITH TOTALS
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'count(non_nullable_column)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, count(b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Large hash table (exercises prefetch path)';
DROP TABLE IF EXISTS test_large;
CREATE TABLE test_large (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_large SELECT number AS a, number AS b FROM numbers(5000000);
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_large GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

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
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

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

INSERT INTO test_sparse_argument
SELECT
    toString(number % 1000) AS a,
    if(number % 10 = 0, number, 0) AS b
FROM numbers(300000);

SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_sparse_argument GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Explicit external aggregation settings are ignored on sharded path';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_sparse_argument GROUP BY a
    SETTINGS enable_sharding_aggregator = 1, max_bytes_before_external_group_by = 1
) WHERE explain LIKE '%ShardByHashTransform%';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_sparse_argument GROUP BY a
    SETTINGS enable_sharding_aggregator = 1, max_bytes_ratio_before_external_group_by = 0.1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Multi-key GROUP BY';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, b % 100 AS k, sum(b) FROM test GROUP BY a, k
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT '-If combinators (sumIf, countIf)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sumIf(b, flag), countIf(flag), max(b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT '-Array combinator (sumArray)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sumArray(arr) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT '-State combinator (sumState)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sumState(b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';
