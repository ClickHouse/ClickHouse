-- Tags: no-fasttest
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.

-- `allow_experimental_xgboost` gates bringing a new XGBOOST dictionary into existence, not reinstating one
-- that already exists. ATTACH must therefore work with the setting off - the metadata is already on disk, and
-- a session whose settings happen to have the setting off must still be able to attach it back.

DROP DICTIONARY IF EXISTS model_04654_xgb;
DROP TABLE IF EXISTS training_04654;

CREATE TABLE training_04654 (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO training_04654 (x1, x2, y) SELECT number AS x1, number * 2 AS x2, 2 * x1 + 3 * x2 AS y FROM numbers(100);

SET allow_experimental_xgboost = 1;

CREATE DICTIONARY model_04654_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_04654'))
LAYOUT(XGBOOST(num_iterations 10))
LIFETIME(0);

DETACH DICTIONARY model_04654_xgb;

SET allow_experimental_xgboost = 0;

-- Short syntax: the definition comes from the stored metadata.
ATTACH DICTIONARY model_04654_xgb;

-- The dictionary is back, and the setting still gates predicting through it.
SELECT predictXGBoost('model_04654_xgb', 1.0, 2.0); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_xgboost = 1;
SELECT isFinite(predictXGBoost('model_04654_xgb', 1.0, 2.0));

-- Full syntax ATTACH, the form a RESTORE replays, is likewise not gated by the setting.
DETACH DICTIONARY model_04654_xgb;
SET allow_experimental_xgboost = 0;

ATTACH DICTIONARY model_04654_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_04654'))
LAYOUT(XGBOOST(num_iterations 10))
LIFETIME(0);

SET allow_experimental_xgboost = 1;
SELECT isFinite(predictXGBoost('model_04654_xgb', 1.0, 2.0));

DROP DICTIONARY model_04654_xgb;
DROP TABLE training_04654;
