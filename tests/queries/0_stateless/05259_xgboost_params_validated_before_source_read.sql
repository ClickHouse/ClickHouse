-- Tags: no-fasttest
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.

-- The training parameters of `LAYOUT(XGBOOST(...))` are validated before the source is read, so a mistake in
-- the layout definition does not cost a scan of the whole training table. The source table below does not
-- exist: reading it would fail with `UNKNOWN_TABLE`, so `BAD_ARGUMENTS` shows that the source was not touched.

SET enable_xgboost = 1;

DROP DICTIONARY IF EXISTS model_05259_xgb;

SELECT 'Valid parameters reach the source';

CREATE DICTIONARY model_05259_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'missing_training_05259'))
LAYOUT(XGBOOST(num_iterations 10))
LIFETIME(0);
SELECT predictXGBoost('model_05259_xgb', 1.0, 2.0); -- { serverError UNKNOWN_TABLE }
DROP DICTIONARY model_05259_xgb;

SELECT 'An unknown parameter is rejected before the source is read';

CREATE DICTIONARY model_05259_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'missing_training_05259'))
LAYOUT(XGBOOST(num_iterations 10 not_a_training_param 1))
LIFETIME(0);
SELECT predictXGBoost('model_05259_xgb', 1.0, 2.0); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY model_05259_xgb;

SELECT 'An invalid num_iterations is rejected before the source is read';

CREATE DICTIONARY model_05259_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'missing_training_05259'))
LAYOUT(XGBOOST(num_iterations 0))
LIFETIME(0);
SELECT predictXGBoost('model_05259_xgb', 1.0, 2.0); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY model_05259_xgb;

SELECT 'A multiclass objective is rejected before the source is read';

CREATE DICTIONARY model_05259_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'missing_training_05259'))
LAYOUT(XGBOOST(objective 'multi:softmax'))
LIFETIME(0);
SELECT predictXGBoost('model_05259_xgb', 1.0, 2.0); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY model_05259_xgb;
