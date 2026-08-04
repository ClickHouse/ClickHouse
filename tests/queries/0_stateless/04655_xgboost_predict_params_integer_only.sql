-- Tags: no-fasttest
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.

-- Every prediction parameter of predictXGBoost is an integer or a boolean, so the trailing params Map must
-- have an integer value type. A fractional value used to be truncated silently - map('iteration_end', 2.9)
-- predicted with two trees instead of reporting the typo - so such a Map is now rejected at query analysis.

SET enable_xgboost = 1;

DROP DICTIONARY IF EXISTS model_04655_xgb;
DROP TABLE IF EXISTS training_04655;

CREATE TABLE training_04655
(
    x1 Float64,
    x2 Float64,
    y Float64
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO training_04655 (x1, x2, y)
SELECT
    number AS x1,
    number * 2 AS x2,
    2 * x1 + 3 * x2 AS y
FROM numbers(100);

CREATE DICTIONARY model_04655_xgb
(
    x1 Float64,
    x2 Float64,
    y Float64
)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_04655'))
LAYOUT(XGBOOST(objective 'reg:squarederror' num_iterations 10 max_depth 3))
LIFETIME(0);

-- Integer parameter values are accepted, including a Bool and a narrower Int type.
SELECT isFinite(predictXGBoost('model_04655_xgb', 1.0, 2.0, map('type', 0, 'iteration_end', 0)));
SELECT isFinite(predictXGBoost('model_04655_xgb', 1.0, 2.0, map('type', false)));
SELECT isFinite(predictXGBoost('model_04655_xgb', 1.0, 2.0, map('iteration_end', toInt8(1))));

-- Fractional values are rejected instead of being truncated.
SELECT predictXGBoost('model_04655_xgb', 1.0, 2.0, map('iteration_end', 2.9)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT predictXGBoost('model_04655_xgb', 1.0, 2.0, map('type', 1.0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT predictXGBoost('model_04655_xgb', 1.0, 2.0, map('iteration_begin', 0.5)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A single fractional value promotes the whole Map to Float64, so the Map as a whole is rejected.
SELECT predictXGBoost('model_04655_xgb', 1.0, 2.0, map('type', 1, 'iteration_end', 2.5)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Decimal and wide integer values are not accepted either.
SELECT predictXGBoost('model_04655_xgb', 1.0, 2.0, map('iteration_end', toDecimal32(1, 2))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT predictXGBoost('model_04655_xgb', 1.0, 2.0, map('iteration_end', toInt128(1))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- An unsigned value that does not fit in Int64 is rejected instead of wrapping around to a negative one.
SELECT predictXGBoost('model_04655_xgb', 1.0, 2.0, map('iteration_end', toUInt64(18446744073709551615))); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY model_04655_xgb;
DROP TABLE training_04655;
