-- Test that sumMap typed path produces correct results under parallel aggregation.
-- Compares serial (max_threads=1) vs parallel (max_threads=8) execution.
-- Any merge bug would cause the results to diverge.

-- { echoOn }

DROP TABLE IF EXISTS test_summap_par;
CREATE TABLE test_summap_par (
    grp UInt32,
    keys Array(UInt32),
    vals Array(UInt64)
) ENGINE = MergeTree ORDER BY grp;

INSERT INTO test_summap_par
SELECT
    number % 5 AS grp,
    [toUInt32(number % 10), toUInt32((number + 3) % 10)] AS keys,
    [toUInt64(1), toUInt64(1)] AS vals
FROM numbers(100000);

-- sumMap: serial vs parallel must match
SELECT 'sumMap serial vs parallel';
SELECT
    (SELECT sumMap(keys, vals) FROM test_summap_par SETTINGS max_threads = 1) =
    (SELECT sumMap(keys, vals) FROM test_summap_par SETTINGS max_threads = 8);

-- sumMap with GROUP BY: serial vs parallel must match
SELECT 'sumMap GROUP BY serial vs parallel';
SELECT
    (SELECT groupArray(t) FROM (SELECT (grp, sumMap(keys, vals)) AS t FROM test_summap_par GROUP BY grp ORDER BY grp) SETTINGS max_threads = 1) =
    (SELECT groupArray(t) FROM (SELECT (grp, sumMap(keys, vals)) AS t FROM test_summap_par GROUP BY grp ORDER BY grp) SETTINGS max_threads = 8);

-- minMap: serial vs parallel
SELECT 'minMap serial vs parallel';
SELECT
    (SELECT minMap(keys, vals) FROM test_summap_par SETTINGS max_threads = 1) =
    (SELECT minMap(keys, vals) FROM test_summap_par SETTINGS max_threads = 8);

-- maxMap: serial vs parallel
SELECT 'maxMap serial vs parallel';
SELECT
    (SELECT maxMap(keys, vals) FROM test_summap_par SETTINGS max_threads = 1) =
    (SELECT maxMap(keys, vals) FROM test_summap_par SETTINGS max_threads = 8);

-- sumMapWithOverflow: serial vs parallel
SELECT 'sumMapWithOverflow serial vs parallel';
SELECT
    (SELECT sumMapWithOverflow(keys, vals) FROM test_summap_par SETTINGS max_threads = 1) =
    (SELECT sumMapWithOverflow(keys, vals) FROM test_summap_par SETTINGS max_threads = 8);

DROP TABLE test_summap_par;

-- String keys: exercises the arena-backed key storage under parallel merge
DROP TABLE IF EXISTS test_summap_par_str;
CREATE TABLE test_summap_par_str (
    keys Array(String),
    vals Array(UInt64)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_summap_par_str
SELECT
    [concat('k', toString(number % 20)), concat('k', toString((number + 7) % 20))] AS keys,
    [toUInt64(1), toUInt64(1)] AS vals
FROM numbers(100000);

SELECT 'String keys serial vs parallel';
SELECT
    (SELECT sumMap(keys, vals) FROM test_summap_par_str SETTINGS max_threads = 1) =
    (SELECT sumMap(keys, vals) FROM test_summap_par_str SETTINGS max_threads = 8);

DROP TABLE test_summap_par_str;

-- Multiple value columns under parallel merge
DROP TABLE IF EXISTS test_summap_par_multi;
CREATE TABLE test_summap_par_multi (
    keys Array(UInt32),
    v1 Array(UInt64),
    v2 Array(Int32)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_summap_par_multi
SELECT
    [toUInt32(number % 15), toUInt32((number + 5) % 15)] AS keys,
    [toUInt64(1), toUInt64(1)] AS v1,
    [toInt32(1), toInt32(-1)] AS v2
FROM numbers(100000);

SELECT 'Multiple value columns serial vs parallel';
SELECT
    (SELECT sumMap(keys, v1, v2) FROM test_summap_par_multi SETTINGS max_threads = 1) =
    (SELECT sumMap(keys, v1, v2) FROM test_summap_par_multi SETTINGS max_threads = 8);

DROP TABLE test_summap_par_multi;

-- Nullable values under parallel merge
DROP TABLE IF EXISTS test_summap_par_nullable;
CREATE TABLE test_summap_par_nullable (
    keys Array(UInt32),
    vals Array(Nullable(UInt64))
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_summap_par_nullable
SELECT
    [toUInt32(number % 15), toUInt32((number + 5) % 15)] AS keys,
    [if(number % 3 = 0, NULL, toUInt64(1))::Nullable(UInt64),
     if(number % 7 = 0, NULL, toUInt64(1))::Nullable(UInt64)] AS vals
FROM numbers(100000);

SELECT 'Nullable values serial vs parallel';
SELECT
    (SELECT sumMap(keys, vals) FROM test_summap_par_nullable SETTINGS max_threads = 1) =
    (SELECT sumMap(keys, vals) FROM test_summap_par_nullable SETTINGS max_threads = 8);

DROP TABLE test_summap_par_nullable;

-- sumMapFiltered under parallel merge
DROP TABLE IF EXISTS test_summap_par_filtered;
CREATE TABLE test_summap_par_filtered (
    keys Array(UInt32),
    vals Array(UInt64)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_summap_par_filtered
SELECT
    [toUInt32(number % 20), toUInt32((number + 3) % 20)] AS keys,
    [toUInt64(1), toUInt64(1)] AS vals
FROM numbers(100000);

SELECT 'sumMapFiltered serial vs parallel';
SELECT
    (SELECT sumMapFiltered([0, 5, 10, 15])(keys, vals) FROM test_summap_par_filtered SETTINGS max_threads = 1) =
    (SELECT sumMapFiltered([0, 5, 10, 15])(keys, vals) FROM test_summap_par_filtered SETTINGS max_threads = 8);

DROP TABLE test_summap_par_filtered;
