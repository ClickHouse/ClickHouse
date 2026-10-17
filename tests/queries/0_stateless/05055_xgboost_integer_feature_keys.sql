-- Tags: no-fasttest, no-parallel-replicas
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.
-- no-parallel-replicas: the dictionary exists only on the initiator, so a query spread over the
-- replicas fails with `Dictionary (model_05055_int) not found` on the others.

-- The XGBOOST layout accepts any native numeric type as a feature (key) column, not only floats:
-- training and prediction both read the features through `getFloat64`. Cover signed and unsigned
-- integer keys, and a dictionary that mixes an integer key with a floating-point one.

SET enable_xgboost = 1;

DROP DICTIONARY IF EXISTS model_05055_int;
DROP DICTIONARY IF EXISTS model_05055_mixed;
DROP TABLE IF EXISTS training_05055_int;
DROP TABLE IF EXISTS training_05055_mixed;

SELECT 'Signed and unsigned integer feature keys';

CREATE TABLE training_05055_int
(
    x1 Int32,
    x2 UInt8,
    y Float64
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO training_05055_int (x1, x2, y)
SELECT
    (number % 10) - 5 AS x1,
    toUInt8(intDiv(number, 10)) AS x2,
    x1 + 10 * x2 AS y
FROM numbers(100);

CREATE DICTIONARY model_05055_int (x1 Int32, x2 UInt8, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05055_int'))
LAYOUT(XGBOOST(num_iterations 200 max_depth 6 seed 42))
LIFETIME(0);

SELECT max(abs(predictXGBoost('model_05055_int', x1, x2) - y)) < 1 FROM training_05055_int;

-- y = x1 + 10 * x2, so y(-5, 0) = -5: a negative target reached through a negative integer key.
SELECT abs(predictXGBoost('model_05055_int', -5, 0) - -5) < 1;
SELECT abs(predictXGBoost('model_05055_int', 4, 9) - 94) < 1;

-- A float argument for an integer key is accepted and gives the same answer, because the feature is
-- read as a float either way.
SELECT predictXGBoost('model_05055_int', 4, 9) = predictXGBoost('model_05055_int', 4.0, 9.0);

SELECT 'An integer key mixed with a floating-point key';

CREATE TABLE training_05055_mixed
(
    x1 Int32,
    x2 Float64,
    y Float64
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO training_05055_mixed (x1, x2, y)
SELECT
    (number % 10) - 5 AS x1,
    intDiv(number, 10) / 2 AS x2,
    x1 + 10 * x2 AS y
FROM numbers(100);

CREATE DICTIONARY model_05055_mixed (x1 Int32, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05055_mixed'))
LAYOUT(XGBOOST(num_iterations 200 max_depth 6 seed 42))
LIFETIME(0);

SELECT max(abs(predictXGBoost('model_05055_mixed', x1, x2) - y)) < 1 FROM training_05055_mixed;

-- y(-5, 4.5) = -5 + 45 = 40
SELECT abs(predictXGBoost('model_05055_mixed', -5, 4.5) - 40) < 1;

DROP DICTIONARY model_05055_int;
DROP DICTIONARY model_05055_mixed;
DROP TABLE training_05055_int;
DROP TABLE training_05055_mixed;
