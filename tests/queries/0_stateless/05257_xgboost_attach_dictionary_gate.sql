-- Tags: no-fasttest
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.

-- The `enable_xgboost` gate applies to definitions supplied by the user. ATTACH DICTIONARY accepts no
-- definition of its own - only the short form - so it replays the stored one and must stay allowed with
-- the setting off, like a server restart does.

DROP DICTIONARY IF EXISTS model_05257_xgb;
DROP TABLE IF EXISTS training_05257;

CREATE TABLE training_05257 (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO training_05257 SELECT number AS x1, intDiv(number, 7) AS x2, 2 * x1 + 3 * x2 AS y FROM numbers(100);

SET enable_xgboost = 1;

CREATE DICTIONARY model_05257_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05257'))
LAYOUT(XGBOOST(num_iterations 10))
LIFETIME(0);

DETACH DICTIONARY model_05257_xgb;

SELECT count() FROM system.dictionaries WHERE database = currentDatabase() AND name = 'model_05257_xgb';

SET enable_xgboost = 0;

-- The short ATTACH replays the stored definition, so it is allowed with the setting off.
ATTACH DICTIONARY model_05257_xgb;

SELECT count() FROM system.dictionaries WHERE database = currentDatabase() AND name = 'model_05257_xgb';

-- The dictionary is back, but still unusable while the setting is off.
SELECT predictXGBoost('model_05257_xgb', 1.0, 2.0); -- { serverError SUPPORT_IS_DISABLED }

SET enable_xgboost = 1;

SELECT abs(predictXGBoost('model_05257_xgb', 1.0, 2.0) - 8) < 5;

DROP DICTIONARY model_05257_xgb;
DROP TABLE training_05257;
