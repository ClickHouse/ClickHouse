-- Tags: long

SET max_rows_to_group_by = 0;

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
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'Numeric expression key';
SELECT
    (SELECT sum(s), count() FROM (SELECT b % 1000 AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT b % 1000 AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'No aggregate functions (aggregates_size == 0)';
SELECT
    (SELECT sum(cityHash64(a)), count() FROM (SELECT a FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(cityHash64(a)), count() FROM (SELECT a FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'count() fast path (is_simple_count)';
SELECT
    (SELECT sum(cnt), count() FROM (SELECT a, count() AS cnt FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(cnt), count() FROM (SELECT a, count() AS cnt FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'Multiple aggregate functions (sum, count, max)';
SELECT
    (SELECT sum(s1), sum(s2), sum(s3), count() FROM
        (SELECT a, sum(b) AS s1, count() AS s2, max(b) AS s3
         FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s1), sum(s2), sum(s3), count() FROM
        (SELECT a, sum(b) AS s1, count() AS s2, max(b) AS s3
         FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'Nullable key';
SELECT
    (SELECT sum(s), count() FROM (SELECT nullable_key, sum(b) AS s FROM test GROUP BY nullable_key SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT nullable_key, sum(b) AS s FROM test GROUP BY nullable_key SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'Nullable key with diverse underlying NULL data';
SELECT
    (SELECT sum(s), count() FROM (SELECT nullIf(a, a) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT nullIf(a, a) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'UInt16 key (key16)';
SELECT
    (SELECT sum(s), count() FROM (SELECT toUInt16(b % 60000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toUInt16(b % 60000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'UInt32 key (key32)';
SELECT
    (SELECT sum(s), count() FROM (SELECT toUInt32(b % 100000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toUInt32(b % 100000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'UInt64 key (key64)';
SELECT
    (SELECT sum(s), count() FROM (SELECT b AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT b AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'FixedString key';
SELECT
    (SELECT sum(s), count() FROM (SELECT toFixedString(a, 10) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toFixedString(a, 10) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'Int16 key';
SELECT
    (SELECT sum(s), count() FROM (SELECT toInt16(b % 30000) - 15000 AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toInt16(b % 30000) - 15000 AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'Int32 key';
SELECT
    (SELECT sum(s), count() FROM (SELECT toInt32(b % 100000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toInt32(b % 100000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'Int64 key';
SELECT
    (SELECT sum(s), count() FROM (SELECT toInt64(b) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toInt64(b) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'Float32 key';
SELECT
    (SELECT sum(s), count() FROM (SELECT toFloat32(b % 1000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toFloat32(b % 1000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'Float64 key';
SELECT
    (SELECT sum(s), count() FROM (SELECT toFloat64(b % 1000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toFloat64(b % 1000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'min';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, min(b) AS s FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, min(b) AS s FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'avg';
SELECT abs(
    (SELECT sum(s) FROM (SELECT a, avg(b) AS s FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0))
    -
    (SELECT sum(s) FROM (SELECT a, avg(b) AS s FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1))
) < 0.001;

SELECT 'any';
SELECT
    (SELECT count() FROM (SELECT a, any(b) AS s FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT count() FROM (SELECT a, any(b) AS s FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'uniq';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, uniq(b) AS s FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, uniq(b) AS s FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'uniqExact';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, uniqExact(b) AS s FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, uniqExact(b) AS s FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'Multi-argument aggregate (argMin)';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, argMin(u8, b) AS s FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, argMin(u8, b) AS s FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'Shared argument across aggregates (sum(b), max(b))';
SELECT
    (SELECT sum(s1), sum(s2), count() FROM (SELECT a, sum(b) AS s1, max(b) AS s2 FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s1), sum(s2), count() FROM (SELECT a, sum(b) AS s1, max(b) AS s2 FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'WITH TOTALS';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a WITH TOTALS SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a WITH TOTALS SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'count(non_nullable_column)';
SELECT
    (SELECT sum(cnt), count() FROM (SELECT a, count(b) AS cnt FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(cnt), count() FROM (SELECT a, count(b) AS cnt FROM test GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'Empty table';
DROP TABLE IF EXISTS test_empty;
CREATE TABLE test_empty (a String, b UInt64) ENGINE = MergeTree ORDER BY tuple();
SELECT
    (SELECT count() FROM (SELECT a, sum(b) AS s FROM test_empty GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT count() FROM (SELECT a, sum(b) AS s FROM test_empty GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'Large hash table (exercises prefetch path)';
DROP TABLE IF EXISTS test_large;
CREATE TABLE test_large (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_large SELECT number AS a, number AS b FROM numbers(5000000);
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test_large GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test_large GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1));

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

SELECT
    (SELECT sum(s), count() FROM (SELECT nullable_key, sum(b) AS s FROM test_sparse GROUP BY nullable_key SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT nullable_key, sum(b) AS s FROM test_sparse GROUP BY nullable_key SETTINGS optimize_aggregation_by_sharding = 1));

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

SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test_sparse_argument GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test_sparse_argument GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1));

SELECT 'Explicit external aggregation settings are ignored on sharded path';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test_sparse_argument GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0, max_bytes_before_external_group_by = 1))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test_sparse_argument GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1, max_bytes_before_external_group_by = 1));
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test_sparse_argument GROUP BY a SETTINGS optimize_aggregation_by_sharding = 0, max_bytes_ratio_before_external_group_by = 0.1))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test_sparse_argument GROUP BY a SETTINGS optimize_aggregation_by_sharding = 1, max_bytes_ratio_before_external_group_by = 0.1));
