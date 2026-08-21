-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

DROP TABLE IF EXISTS test_negate_nan_ne_float;

CREATE TABLE test_negate_nan_ne_float
(
    p Float64
)
ENGINE = MergeTree
ORDER BY negate(p)
SETTINGS index_granularity = 1;

INSERT INTO test_negate_nan_ne_float VALUES (1.0), (toFloat64('nan')), (2.0);

SELECT count(), sum(isNaN(p)) FROM test_negate_nan_ne_float WHERE negate(p) != -3;

EXPLAIN indexes = 1
SELECT count(), sum(isNaN(p)) FROM test_negate_nan_ne_float WHERE negate(p) != -3;


DROP TABLE IF EXISTS test_nan_ne_float;

CREATE TABLE test_nan_ne_float
(
    p Float64
)
ENGINE = MergeTree
ORDER BY negate(p)
SETTINGS index_granularity = 1;

INSERT INTO test_nan_ne_float VALUES (1.0), (toFloat64('nan')), (2.0);

SELECT
    count(),
    sum(isNaN(p))
FROM test_nan_ne_float
WHERE p != toFloat64('3');

EXPLAIN indexes = 1
SELECT
    count(),
    sum(isNaN(p))
FROM test_nan_ne_float
WHERE p != toFloat64('3');


DROP TABLE IF EXISTS test_nan_ne_nan;

CREATE TABLE test_nan_ne_nan
(
    p Float64
)
ENGINE = MergeTree
ORDER BY toString(p)
SETTINGS index_granularity = 1;

INSERT INTO test_nan_ne_nan
SELECT if(number < 5, toFloat64('nan'), toFloat64(number))
FROM numbers(10);

SELECT count() AS cnt, sum(isNaN(p)) AS nan_rows
FROM test_nan_ne_nan
WHERE p != toFloat64('nan');

EXPLAIN indexes = 1
SELECT count(), sum(isNaN(p))
FROM test_nan_ne_nan
WHERE p != toFloat64('nan');

DROP TABLE IF EXISTS test_normal_less_nan;

CREATE TABLE test_normal_less_nan
(
    x Float64
)
ENGINE = MergeTree
ORDER BY x
SETTINGS index_granularity = 1;

INSERT INTO test_normal_less_nan
SELECT toFloat64(number)
FROM numbers(20);

SELECT count() FROM test_normal_less_nan WHERE x < toFloat64('nan');

EXPLAIN indexes = 1
SELECT count() FROM test_normal_less_nan WHERE x < toFloat64('nan');

SELECT count() FROM test_normal_less_nan WHERE NOT (x < toFloat64('nan'));

EXPLAIN indexes = 1
SELECT count() FROM test_normal_less_nan WHERE NOT (x < toFloat64('nan'));
