-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- EXPLAIN output may differ

SET max_rows_to_group_by = 0;
SET max_threads = 8;

DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    rn UInt64,
    k_str String,
    k_fs FixedString(8),
    k_dec Decimal64(2),
    k_date Date,
    k_null Nullable(UInt64),
    k_sparse UInt64,
    k_dense UInt64,
    v Int64
)
ENGINE = MergeTree ORDER BY rn
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;

-- Each key column has one dominant (~90%) hot value over a high-cardinality cold tail. k_null's hot value
-- is NULL (membership uses transform_null_in); k_sparse's is the default 0 (serialized sparse), k_dense's
-- a non-default constant.
INSERT INTO test
SELECT
    number AS rn,
    (number % 10 < 9) ? 'HOT' : toString(number % 100000) AS k_str,
    (number % 10 < 9) ? toFixedString('HOTVALUE', 8) : toFixedString(toString(number % 100000), 8) AS k_fs,
    (number % 10 < 9) ? toDecimal64(7777.77, 2) : toDecimal64(number % 100000, 2) AS k_dec,
    (number % 10 < 9) ? toDate('2021-06-15') : (toDate('2000-01-01') + toInt32(number % 10000)) AS k_date,
    (number % 10 < 9) ? NULL : (number % 100000) AS k_null,
    (number % 10 < 9) ? 0 : (number % 100000) AS k_sparse,
    (number % 10 < 9) ? 999999 : (number % 100000) AS k_dense,
    toInt64(number) AS v
FROM numbers(1000000);

SELECT 'String key';
SELECT (SELECT sum(s), count() FROM (SELECT k_str, sum(v) AS s FROM test GROUP BY k_str SETTINGS enable_sharding_aggregator = 0))
     = (SELECT sum(s), count() FROM (SELECT k_str, sum(v) AS s FROM test GROUP BY k_str SETTINGS enable_sharding_aggregator = 1));

SELECT 'FixedString key';
SELECT (SELECT sum(s), count() FROM (SELECT k_fs, sum(v) AS s FROM test GROUP BY k_fs SETTINGS enable_sharding_aggregator = 0))
     = (SELECT sum(s), count() FROM (SELECT k_fs, sum(v) AS s FROM test GROUP BY k_fs SETTINGS enable_sharding_aggregator = 1));

SELECT 'Decimal key';
SELECT (SELECT sum(s), count() FROM (SELECT k_dec, sum(v) AS s FROM test GROUP BY k_dec SETTINGS enable_sharding_aggregator = 0))
     = (SELECT sum(s), count() FROM (SELECT k_dec, sum(v) AS s FROM test GROUP BY k_dec SETTINGS enable_sharding_aggregator = 1));

SELECT 'Date key';
SELECT (SELECT sum(s), count() FROM (SELECT k_date, sum(v) AS s FROM test GROUP BY k_date SETTINGS enable_sharding_aggregator = 0))
     = (SELECT sum(s), count() FROM (SELECT k_date, sum(v) AS s FROM test GROUP BY k_date SETTINGS enable_sharding_aggregator = 1));

SELECT 'Nullable key with NULL hot value';
SELECT (SELECT sum(s), count() FROM (SELECT k_null, sum(v) AS s FROM test GROUP BY k_null SETTINGS enable_sharding_aggregator = 0))
     = (SELECT sum(s), count() FROM (SELECT k_null, sum(v) AS s FROM test GROUP BY k_null SETTINGS enable_sharding_aggregator = 1));

SELECT 'NULL group finalized exactly once (vs independent oracle)';
SELECT c = (SELECT count() FROM numbers(1000000) WHERE number % 10 < 9)
FROM (SELECT k_null, count() AS c FROM test GROUP BY k_null SETTINGS enable_sharding_aggregator = 1)
WHERE k_null IS NULL;

SELECT 'Sparse (default-dominated) hot key';
SELECT (SELECT sum(s), count() FROM (SELECT k_sparse, sum(v) AS s FROM test GROUP BY k_sparse SETTINGS enable_sharding_aggregator = 0))
     = (SELECT sum(s), count() FROM (SELECT k_sparse, sum(v) AS s FROM test GROUP BY k_sparse SETTINGS enable_sharding_aggregator = 1));

SELECT 'Dense (non-default) hot key';
SELECT (SELECT sum(s), count() FROM (SELECT k_dense, sum(v) AS s FROM test GROUP BY k_dense SETTINGS enable_sharding_aggregator = 0))
     = (SELECT sum(s), count() FROM (SELECT k_dense, sum(v) AS s FROM test GROUP BY k_dense SETTINGS enable_sharding_aggregator = 1));

SELECT 'Pipeline contains skew-split transforms';
SELECT countIf(explain LIKE '%BufferedScatterTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k_str, count() FROM test GROUP BY k_str SETTINGS enable_sharding_aggregator = 1);

DROP TABLE test;
