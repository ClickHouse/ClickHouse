-- Exercises the compaction of the adaptive aggregator's row-reference staging: a block gathers
-- the staged records' argument values into dense columns at publish and releases the source
-- block. Sparse arguments are materialized by the gather, so the staged columns are always
-- dense. The skewed table stages only the rare tail (one key covers 90% of the rows), the
-- uniform queries stage nearly every row. Every cell compares the same query with the feature
-- off and on.

SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 128;
SET max_threads = 4;
SET max_block_size = 8192;
SET group_by_two_level_threshold = 10000;
SET group_by_two_level_threshold_bytes = 5000000;

DROP TABLE IF EXISTS test_skew;
CREATE TABLE test_skew (k UInt64, v UInt64, s String, nv Nullable(UInt64), arr Array(UInt64), flag UInt8)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_skew
SELECT
    if(number % 10 != 0, 4242424242, number) AS k,
    number AS v,
    concat(repeat('x', 20), toString(number % 1000)) AS s,
    if(number % 7 = 0, NULL, number % 1000) AS nv,
    [number % 3, number % 5] AS arr,
    toUInt8(number % 2) AS flag
FROM numbers(200000);

SELECT 'Skewed sum (compaction fires)';
SELECT
    (SELECT count(), sum(c), sum(sm) FROM (SELECT k, count() AS c, sum(v) AS sm FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(c), sum(sm) FROM (SELECT k, count() AS c, sum(v) AS sm FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Skewed key that is also an argument';
SELECT
    (SELECT count(), sum(c), sum(sm) FROM (SELECT k, count() AS c, sum(k) AS sm FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(c), sum(sm) FROM (SELECT k, count() AS c, sum(k) AS sm FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Skewed String argument';
SELECT
    (SELECT count(), sum(cityHash64(mn)) FROM (SELECT k, min(s) AS mn FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(cityHash64(mn)) FROM (SELECT k, min(s) AS mn FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Skewed Nullable argument';
SELECT
    (SELECT count(), sum(c), sum(sm) FROM (SELECT k, count(nv) AS c, sum(nv) AS sm FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(c), sum(sm) FROM (SELECT k, count(nv) AS c, sum(nv) AS sm FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Skewed constant argument';
SELECT
    (SELECT count(), sum(c), sum(sm) FROM (SELECT k, countIf(1) AS c, sumIf(v, 1) AS sm FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(c), sum(sm) FROM (SELECT k, countIf(1) AS c, sumIf(v, 1) AS sm FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Skewed -If and -Array combinators and argMin (unique order column: ties are settled in processing order, which differs between the paths)';
SELECT
    (SELECT count(), sum(s1), sum(s2), sum(s3) FROM
        (SELECT k, sumIf(v, flag) AS s1, sumArray(arr) AS s2, argMin(v % 7, v) AS s3 FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s1), sum(s2), sum(s3) FROM
        (SELECT k, sumIf(v, flag) AS s1, sumArray(arr) AS s2, argMin(v % 7, v) AS s3 FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Skewed with threshold 0 and 1';
SELECT
    (SELECT count(), sum(sm) FROM (SELECT k, sum(v) AS sm FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(sm) FROM (SELECT k, sum(v) AS sm FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0));
SELECT
    (SELECT count(), sum(sm) FROM (SELECT k, sum(v) AS sm FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(sm) FROM (SELECT k, sum(v) AS sm FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 1));

SELECT 'Uniform high-cardinality sum (every row staged)';
SELECT
    (SELECT count(), sum(c), sum(sm) FROM (SELECT v AS k, count() AS c, sum(v) AS sm FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(c), sum(sm) FROM (SELECT v AS k, count() AS c, sum(v) AS sm FROM test_skew GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Sparse argument is materialized at publish; dense and zero-argument aggregates share the gather';
DROP TABLE IF EXISTS test_skew_sparse;
CREATE TABLE test_skew_sparse (k UInt64, v UInt64, w UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;
INSERT INTO test_skew_sparse
SELECT if(number % 10 != 0, 4242424242, number) AS k, if(number % 20 = 0, number, 0) AS v, number AS w
FROM numbers(200000);
SELECT
    (SELECT count(), sum(c), sum(sm), sum(mw) FROM (SELECT k, count() AS c, sum(v) AS sm, max(w) AS mw FROM test_skew_sparse GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(c), sum(sm), sum(mw) FROM (SELECT k, count() AS c, sum(v) AS sm, max(w) AS mw FROM test_skew_sparse GROUP BY k SETTINGS enable_adaptive_aggregator = 1));
DROP TABLE test_skew_sparse;

DROP TABLE test_skew;

-- Absolute-value guard over deterministic data: 90% of the rows share one hot key, the tail keys
-- are the multiples of ten, so every aggregate below is analytically known.
SELECT 'Analytic guard: skewed sum over numbers_mt';
SELECT count(), sum(c), sum(sm)
FROM
(
    SELECT if(number % 10 != 0, 4242424242, number) AS k, count() AS c, sum(number) AS sm
    FROM numbers_mt(200000)
    GROUP BY k
    SETTINGS enable_adaptive_aggregator = 1
);
