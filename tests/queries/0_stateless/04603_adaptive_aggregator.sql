-- Tags: long
-- Random settings limits: index_granularity=(8192, None)
-- A tiny index_granularity over S3 storage explodes the per-granule read count
-- (one S3 GET per granule for multi-column reads), making this long test time out.

-- Exercises the adaptive aggregation algorithm: each thread aggregates into its local hash table
-- until it holds `adaptive_aggregator_freeze_threshold` keys, then the table freezes, so hits keep
-- updating it in place while misses are staged as delayed records routed by the two-level bucket
-- of the key hash and folded in during the bucket-parallel merge. Every cell compares the same
-- query with the feature off and on, so the expected output is a column of 1s regardless of the
-- randomly generated table content.

SET max_rows_to_group_by = 0;
SET max_threads = 4;
SET max_block_size = 8192;
SET enable_sharding_aggregator = 0;
SET adaptive_aggregator_freeze_threshold = 128;
-- The adaptive gate requires two-level aggregation to be permitted.
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 50000000;

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
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Numeric expression key';
SELECT
    (SELECT sum(s), count() FROM (SELECT b % 1000 AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT b % 1000 AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'No aggregate functions (aggregates_size == 0)';
SELECT
    (SELECT sum(cityHash64(a)), count() FROM (SELECT a FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(cityHash64(a)), count() FROM (SELECT a FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1));

SELECT 'count() fast path (is_simple_count, value-staged records)';
SELECT
    (SELECT sum(cnt), count() FROM (SELECT a, count() AS cnt FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(cnt), count() FROM (SELECT a, count() AS cnt FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1));

SELECT 'count() with UInt64 key (low cardinality, consecutive keys cache)';
SELECT
    (SELECT sum(cnt), count() FROM (SELECT toUInt64(b % 100) AS k, count() AS cnt FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(cnt), count() FROM (SELECT toUInt64(b % 100) AS k, count() AS cnt FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Multiple aggregate functions (sum, count, max)';
SELECT
    (SELECT sum(s1), sum(s2), sum(s3), count() FROM
        (SELECT a, sum(b) AS s1, count() AS s2, max(b) AS s3
         FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s1), sum(s2), sum(s3), count() FROM
        (SELECT a, sum(b) AS s1, count() AS s2, max(b) AS s3
         FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Nullable key (not admitted, results must still be correct)';
SELECT
    (SELECT sum(s), count() FROM (SELECT nullable_key, sum(b) AS s FROM test GROUP BY nullable_key SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT nullable_key, sum(b) AS s FROM test GROUP BY nullable_key SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Nullable key with diverse underlying NULL data';
SELECT
    (SELECT sum(s), count() FROM (SELECT nullIf(a, a) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT nullIf(a, a) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'UInt8 key (key8, not admitted)';
SELECT
    (SELECT sum(s), count() FROM (SELECT u8, sum(b) AS s FROM test GROUP BY u8 SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT u8, sum(b) AS s FROM test GROUP BY u8 SETTINGS enable_adaptive_aggregator = 1));

SELECT 'UInt16 key (key16, not admitted)';
SELECT
    (SELECT sum(s), count() FROM (SELECT toUInt16(b % 60000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toUInt16(b % 60000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'UInt32 key (key32)';
SELECT
    (SELECT sum(s), count() FROM (SELECT toUInt32(b % 100000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toUInt32(b % 100000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'UInt64 key (key64)';
SELECT
    (SELECT sum(s), count() FROM (SELECT b AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT b AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Int16 key (not admitted)';
SELECT
    (SELECT sum(s), count() FROM (SELECT toInt16(b % 30000) - 15000 AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toInt16(b % 30000) - 15000 AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Int32 key';
SELECT
    (SELECT sum(s), count() FROM (SELECT toInt32(b % 100000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toInt32(b % 100000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Int64 key';
SELECT
    (SELECT sum(s), count() FROM (SELECT toInt64(b) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toInt64(b) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Float32 key';
SELECT
    (SELECT sum(s), count() FROM (SELECT toFloat32(b % 1000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toFloat32(b % 1000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Float64 key';
SELECT
    (SELECT sum(s), count() FROM (SELECT toFloat64(b % 1000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toFloat64(b % 1000) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Float64 key with NaN and signed zeros';
SELECT
    (SELECT sum(s), count() FROM (SELECT if(b % 100 = 0, nan, if(b % 2 = 0, 0., -0.) + toFloat64(b % 1000)) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT if(b % 100 = 0, nan, if(b % 2 = 0, 0., -0.) + toFloat64(b % 1000)) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'FixedString key';
SELECT
    (SELECT sum(s), count() FROM (SELECT toFixedString(a, 10) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toFixedString(a, 10) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'LowCardinality(String) key (not admitted)';
SELECT
    (SELECT sum(s), count() FROM (SELECT toLowCardinality(a) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT toLowCardinality(a) AS k, sum(b) AS s FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'String key crossing every length class (1..30 bytes)';
SELECT
    (SELECT sum(cityHash64(k, cnt)), count() FROM (SELECT concat(substring(repeat('abcdefgh', 4), 1, 1 + b % 30), a) AS k, count() AS cnt FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(cityHash64(k, cnt)), count() FROM (SELECT concat(substring(repeat('abcdefgh', 4), 1, 1 + b % 30), a) AS k, count() AS cnt FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'String key at the exact 8/16/24-byte boundaries, with and without a trailing zero byte';
SELECT
    (SELECT sum(cityHash64(k, cnt)), count() FROM
        (SELECT arrayJoin([
            leftPad(toString(b % 300), 8, '0'),
            leftPad(toString(b % 300), 16, '0'),
            leftPad(toString(b % 300), 24, '0'),
            leftPad(toString(b % 300), 25, '0'),
            concat(leftPad(toString(b % 300), 7, '0'), unhex('00')),
            concat(leftPad(toString(b % 300), 15, '0'), unhex('00')),
            concat(leftPad(toString(b % 300), 23, '0'), unhex('00'))]) AS k, count() AS cnt
         FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(cityHash64(k, cnt)), count() FROM
        (SELECT arrayJoin([
            leftPad(toString(b % 300), 8, '0'),
            leftPad(toString(b % 300), 16, '0'),
            leftPad(toString(b % 300), 24, '0'),
            leftPad(toString(b % 300), 25, '0'),
            concat(leftPad(toString(b % 300), 7, '0'), unhex('00')),
            concat(leftPad(toString(b % 300), 15, '0'), unhex('00')),
            concat(leftPad(toString(b % 300), 23, '0'), unhex('00'))]) AS k, count() AS cnt
         FROM test GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'min';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, min(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, min(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1));

SELECT 'avg';
SELECT abs(
    (SELECT sum(s) FROM (SELECT a, avg(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    -
    (SELECT sum(s) FROM (SELECT a, avg(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1))
) < 0.001;

SELECT 'any';
SELECT
    (SELECT count() FROM (SELECT a, any(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count() FROM (SELECT a, any(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1));

SELECT 'uniq';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, uniq(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, uniq(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1));

SELECT 'uniqExact (aggregate states with arena allocations)';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, uniqExact(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, uniqExact(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Multi-argument aggregate (argMin)';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, argMin(u8, b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, argMin(u8, b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Shared argument across aggregates (sum(b), max(b))';
SELECT
    (SELECT sum(s1), sum(s2), count() FROM (SELECT a, sum(b) AS s1, max(b) AS s2 FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s1), sum(s2), count() FROM (SELECT a, sum(b) AS s1, max(b) AS s2 FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1));

SELECT 'WITH TOTALS';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a WITH TOTALS SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a WITH TOTALS SETTINGS enable_adaptive_aggregator = 1));

SELECT 'count(non_nullable_column)';
SELECT
    (SELECT sum(cnt), count() FROM (SELECT a, count(b) AS cnt FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(cnt), count() FROM (SELECT a, count(b) AS cnt FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Multi-key GROUP BY (String + numeric, serialized method)';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, b % 100 AS k, sum(b) AS s FROM test GROUP BY a, k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, b % 100 AS k, sum(b) AS s FROM test GROUP BY a, k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Multi-key GROUP BY (two numeric keys, packed fixed-size method)';
SELECT
    (SELECT sum(s), count() FROM (SELECT b % 1000 AS k1, intDiv(b, 1000) % 200 AS k2, sum(b) AS s FROM test GROUP BY k1, k2 SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT b % 1000 AS k1, intDiv(b, 1000) % 200 AS k2, sum(b) AS s FROM test GROUP BY k1, k2 SETTINGS enable_adaptive_aggregator = 1));

SELECT '-If combinators (sumIf, countIf)';
SELECT
    (SELECT sum(s1), sum(s2), sum(s3), count() FROM
        (SELECT a, sumIf(b, flag) AS s1, countIf(flag) AS s2, max(b) AS s3
         FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s1), sum(s2), sum(s3), count() FROM
        (SELECT a, sumIf(b, flag) AS s1, countIf(flag) AS s2, max(b) AS s3
         FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1));

SELECT '-Array combinator (sumArray)';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sumArray(arr) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, sumArray(arr) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Constant key, pure count (whole-block run-length record)';
SELECT
    (SELECT k, c FROM (SELECT 'ck' AS k, count() AS c FROM test GROUP BY k) SETTINGS enable_adaptive_aggregator = 0, adaptive_aggregator_freeze_threshold = 0)
    =
    (SELECT k, c FROM (SELECT 'ck' AS k, count() AS c FROM test GROUP BY k) SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0);

SELECT 'Constant key, general aggregates';
SELECT
    (SELECT k, c, s FROM (SELECT 'ck' AS k, count() AS c, sum(b) AS s FROM test GROUP BY k) SETTINGS enable_adaptive_aggregator = 0, adaptive_aggregator_freeze_threshold = 0)
    =
    (SELECT k, c, s FROM (SELECT 'ck' AS k, count() AS c, sum(b) AS s FROM test GROUP BY k) SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0);

SELECT 'Threshold 0 freezes at the first opportunity';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0));

SELECT 'Threshold 1 freezes after the first block';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 1));

SELECT 'GROUP BY with LIMIT (early cancellation of the merge)';
SELECT
    (SELECT count() FROM (SELECT a, count() FROM test GROUP BY a LIMIT 100 SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count() FROM (SELECT a, count() FROM test GROUP BY a LIMIT 100 SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Empty table';
DROP TABLE IF EXISTS test_empty;
CREATE TABLE test_empty (a String, b UInt64) ENGINE = MergeTree ORDER BY tuple();
SELECT
    (SELECT count() FROM (SELECT a, sum(b) AS s FROM test_empty GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count() FROM (SELECT a, sum(b) AS s FROM test_empty GROUP BY a SETTINGS enable_adaptive_aggregator = 1));
DROP TABLE test_empty;

SELECT 'Large hash table (exercises prefetch and the drain reserve)';
DROP TABLE IF EXISTS test_large;
CREATE TABLE test_large (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_large SELECT number AS a, number AS b FROM numbers(5000000);
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test_large GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test_large GROUP BY a SETTINGS enable_adaptive_aggregator = 1));
DROP TABLE test_large;

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
    (SELECT sum(s), count() FROM (SELECT nullable_key, sum(b) AS s FROM test_sparse GROUP BY nullable_key SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT nullable_key, sum(b) AS s FROM test_sparse GROUP BY nullable_key SETTINGS enable_adaptive_aggregator = 1));
DROP TABLE test_sparse;

SELECT 'Sparse aggregate argument';
DROP TABLE IF EXISTS test_sparse_argument;
CREATE TABLE test_sparse_argument
(
    a UInt64,
    v UInt64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;

INSERT INTO test_sparse_argument SELECT number, if(number % 10 = 0, number, 0) FROM numbers(500000);
SELECT
    (SELECT sum(s), count() FROM (SELECT a % 50000 AS g, sum(v) AS s FROM test_sparse_argument GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a % 50000 AS g, sum(v) AS s FROM test_sparse_argument GROUP BY g SETTINGS enable_adaptive_aggregator = 1));
DROP TABLE test_sparse_argument;

SELECT 'Explicit external aggregation settings are ignored on the adaptive path';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1));
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a SETTINGS enable_adaptive_aggregator = 1, max_bytes_ratio_before_external_group_by = 0.1));

DROP TABLE test;

-- Absolute-value guard: self-comparing cells cannot catch a defect shared by both paths, so pin
-- two adaptive results to analytically-known values over deterministic data.
SELECT 'Analytic guard: count per key over numbers_mt';
SELECT count(), sum(c) FROM (SELECT number % 100000 AS k, count() AS c FROM numbers_mt(1000000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1);
SELECT 'Analytic guard: general aggregates over numbers_mt';
SELECT count(), sum(c), sum(s), min(mn), max(mx)
FROM
(
    SELECT number % 100000 AS k, count() AS c, sum(number) AS s, min(number) AS mn, max(number) AS mx
    FROM numbers_mt(1000000)
    GROUP BY k
    SETTINGS enable_adaptive_aggregator = 1
);
