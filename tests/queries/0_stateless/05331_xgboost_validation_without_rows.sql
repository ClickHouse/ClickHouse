-- Tags: no-fasttest
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.

-- An invalid `predictXGBoost` call fails the same way with or without rows to predict. The number of features
-- and the prediction parameters are checked while the query is analysed, from the dictionary structure, so a
-- call over no rows is rejected too, and still does not load (train) the dictionary.

SET enable_xgboost = 1;

DROP DICTIONARY IF EXISTS model_05331_xgb;
DROP TABLE IF EXISTS training_05331;

CREATE TABLE training_05331 (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO training_05331 SELECT number AS x1, intDiv(number, 7) AS x2, 2 * x1 + 3 * x2 AS y FROM numbers(100);

CREATE DICTIONARY model_05331_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05331'))
LAYOUT(XGBOOST(num_iterations 10))
LIFETIME(0);

SELECT 'Invalid calls over no rows';
SELECT predictXGBoost('model_05331_xgb', toFloat64(number)) FROM numbers(0); -- { serverError BAD_ARGUMENTS }
SELECT predictXGBoost('model_05331_xgb', toFloat64(number), 2.0, 3.0) FROM numbers(0); -- { serverError BAD_ARGUMENTS }
SELECT predictXGBoost('model_05331_xgb', toFloat64(number), 2.0, map('type', 2)) FROM numbers(0); -- { serverError BAD_ARGUMENTS }
SELECT predictXGBoost('model_05331_xgb', toFloat64(number), 2.0, map('not_a_predict_param', 1)) FROM numbers(0); -- { serverError BAD_ARGUMENTS }
SELECT predictXGBoost('model_05331_xgb', toFloat64(number), 2.0, map('iteration_end', -1)) FROM numbers(0); -- { serverError BAD_ARGUMENTS }
SELECT predictXGBoost('model_05331_xgb', toFloat64(number), 2.0, map('iteration_end', 0, 'iteration_end', 1)) FROM numbers(0); -- { serverError BAD_ARGUMENTS }

SELECT 'A valid call over no rows returns nothing and does not load the dictionary';
SELECT predictXGBoost('model_05331_xgb', toFloat64(number), 2.0, map('type', 1)) FROM numbers(0);
SELECT status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'model_05331_xgb';

SELECT 'The same invalid calls with rows';
SELECT predictXGBoost('model_05331_xgb', 1.0); -- { serverError BAD_ARGUMENTS }
SELECT predictXGBoost('model_05331_xgb', 1.0, 2.0, 3.0); -- { serverError BAD_ARGUMENTS }
SELECT predictXGBoost('model_05331_xgb', 1.0, 2.0, map('type', 2)); -- { serverError BAD_ARGUMENTS }
SELECT predictXGBoost('model_05331_xgb', 1.0, 2.0, map('not_a_predict_param', 1)); -- { serverError BAD_ARGUMENTS }
SELECT predictXGBoost('model_05331_xgb', 1.0, 2.0, map('iteration_end', -1)); -- { serverError BAD_ARGUMENTS }
SELECT predictXGBoost('model_05331_xgb', 1.0, 2.0, map('iteration_end', 0, 'iteration_end', 1)); -- { serverError BAD_ARGUMENTS }

-- The upper bound of `iteration_end` needs the trained model, so it is checked when there are rows to predict.
SELECT predictXGBoost('model_05331_xgb', 1.0, 2.0, map('iteration_end', 11)); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY model_05331_xgb;
DROP TABLE training_05331;
