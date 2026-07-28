-- Tags: no-fasttest
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.

-- The XGBoost integration (the XGBOOST dictionary layout and the predictXGBoost function) is experimental
-- and gated behind the `allow_experimental_xgboost` setting. With the setting off (the default) both the
-- CREATE DICTIONARY and the function must be rejected.

DROP DICTIONARY IF EXISTS model_04603_xgb;
DROP TABLE IF EXISTS training_04603;

CREATE TABLE training_04603 (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO training_04603 (x1, x2, y) SELECT number as x1, number * 2 as x2, 2 * x1 + 3 * x2 as y FROM numbers(100);

SET allow_experimental_xgboost = 0;

-- Error: creating an XGBOOST dictionary is disabled.
CREATE DICTIONARY model_04603_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_04603'))
LAYOUT(XGBOOST())
LIFETIME(0); -- { serverError SUPPORT_IS_DISABLED }

-- With the setting on, the dictionary is created and predicts successfully.
SET allow_experimental_xgboost = 1;

CREATE DICTIONARY model_04603_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_04603'))
LAYOUT(XGBOOST(num_iterations 10))
LIFETIME(0);

SELECT isFinite(predictXGBoost('model_04603_xgb', 1.0, 2.0));

-- Turning the setting back off must reject predictXGBoost even against an already-created dictionary.
SET allow_experimental_xgboost = 0;

SELECT predictXGBoost('model_04603_xgb', 1.0, 2.0); -- { serverError SUPPORT_IS_DISABLED }

DROP DICTIONARY model_04603_xgb;
DROP TABLE training_04603;
