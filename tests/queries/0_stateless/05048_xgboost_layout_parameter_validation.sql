-- Tags: no-fasttest
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.

-- Validation of the training parameters passed in `LAYOUT(XGBOOST(...))`.
--
-- A layout parameter travels from the DDL literal into the dictionary configuration and is only
-- checked when the dictionary loads. With lazy loading that is the first `predictXGBoost` call, not
-- `CREATE DICTIONARY`, so the rejections below are asserted on the prediction. The exception is a
-- value that the dictionary DDL itself cannot represent, which fails while the query is parsed.

SET enable_xgboost = 1;

DROP TABLE IF EXISTS training_05048;

CREATE TABLE training_05048
(
    x1 Float64,
    x2 Float64,
    y Float64
)
ENGINE = MergeTree
ORDER BY tuple();

-- Independent features, so that a change to a tree-shape parameter is observable.
INSERT INTO training_05048 (x1, x2, y)
SELECT
    number AS x1,
    intDiv(number, 7) AS x2,
    2 * x1 + 3 * x2 AS y
FROM numbers(100);

SELECT 'A training parameter given more than once is rejected';

DROP DICTIONARY IF EXISTS model_05048_duplicate;
CREATE DICTIONARY model_05048_duplicate (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05048'))
LAYOUT(XGBOOST(num_iterations 10 max_depth 4 max_depth 8))
LIFETIME(0);
-- Without an explicit check the repeated key reaches the allowlist as `max_depth[1]`, which is how
-- Poco reports a repeated configuration element, and the error would name that instead of `max_depth`.
SELECT predictXGBoost('model_05048_duplicate', 1.0, 2.0); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY model_05048_duplicate;

SELECT 'eval_metric is not an accepted training parameter';

-- `eval_metric` only affects the output of `XGBoosterEvalOneIter`, which training never calls, so it
-- is not in the allowlist: accepting it would silently ignore it.
DROP DICTIONARY IF EXISTS model_05048_eval_metric;
CREATE DICTIONARY model_05048_eval_metric (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05048'))
LAYOUT(XGBOOST(num_iterations 10 eval_metric 'mae'))
LIFETIME(0);
SELECT predictXGBoost('model_05048_eval_metric', 1.0, 2.0); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY model_05048_eval_metric;

SELECT 'An unknown training parameter is rejected';

DROP DICTIONARY IF EXISTS model_05048_unknown;
CREATE DICTIONARY model_05048_unknown (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05048'))
LAYOUT(XGBOOST(num_iterations 10 not_a_real_param 1))
LIFETIME(0);
SELECT predictXGBoost('model_05048_unknown', 1.0, 2.0); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY model_05048_unknown;

SELECT 'A negative parameter value cannot be expressed in the dictionary DDL';

-- The dictionary DDL accepts only UInt64, Float64 and String layout parameter values, so a negative
-- literal is rejected while the CREATE is converted, before the layout ever sees it.
DROP DICTIONARY IF EXISTS model_05048_negative;
CREATE DICTIONARY model_05048_negative (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05048'))
LAYOUT(XGBOOST(num_iterations 10 seed -1))
LIFETIME(0); -- { serverError BAD_ARGUMENTS }

SELECT 'Parameter names are case-insensitive, and the value is applied';

DROP DICTIONARY IF EXISTS model_05048_lower;
CREATE DICTIONARY model_05048_lower (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05048'))
LAYOUT(XGBOOST(num_iterations 10 max_depth 2))
LIFETIME(0);

DROP DICTIONARY IF EXISTS model_05048_upper;
CREATE DICTIONARY model_05048_upper (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05048'))
LAYOUT(XGBOOST(NUM_ITERATIONS 10 MAX_DEPTH 2))
LIFETIME(0);

DROP DICTIONARY IF EXISTS model_05048_deep;
CREATE DICTIONARY model_05048_deep (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05048'))
LAYOUT(XGBOOST(num_iterations 10 max_depth 8))
LIFETIME(0);

-- The same parameter in a different case trains the same model, so the name is not case-sensitive.
SELECT predictXGBoost('model_05048_lower', 1.0, 2.0) = predictXGBoost('model_05048_upper', 1.0, 2.0);
-- A different value trains a different model, so the parameter really reaches the booster and the
-- equality above is not just two dictionaries both ignoring an unrecognised name.
SELECT predictXGBoost('model_05048_lower', 1.0, 2.0) != predictXGBoost('model_05048_deep', 1.0, 2.0);

DROP DICTIONARY model_05048_lower;
DROP DICTIONARY model_05048_upper;
DROP DICTIONARY model_05048_deep;

DROP TABLE training_05048;
