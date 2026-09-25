-- Tags: no-fasttest
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.

-- A full-definition ATTACH DICTIONARY is fresh user input, so with `enable_xgboost` off it must be rejected
-- for the XGBOOST layout just like CREATE DICTIONARY. A short ATTACH of an already stored dictionary is a
-- replay of its definition and stays allowed.

DROP DICTIONARY IF EXISTS model_05257_xgb;
DROP TABLE IF EXISTS training_05257;

CREATE TABLE training_05257 (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO training_05257 SELECT number AS x1, intDiv(number, 7) AS x2, 2 * x1 + 3 * x2 AS y FROM numbers(100);

SET enable_xgboost = 0;

-- Error: the full-definition ATTACH must not bypass the gate.
ATTACH DICTIONARY model_05257_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05257'))
LAYOUT(XGBOOST(num_iterations 10))
LIFETIME(0); -- { serverError SUPPORT_IS_DISABLED }

SELECT count() FROM system.dictionaries WHERE database = currentDatabase() AND name = 'model_05257_xgb';

-- With the setting on, the full-definition ATTACH succeeds.
SET enable_xgboost = 1;

ATTACH DICTIONARY model_05257_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05257'))
LAYOUT(XGBOOST(num_iterations 10))
LIFETIME(0);

SELECT count() FROM system.dictionaries WHERE database = currentDatabase() AND name = 'model_05257_xgb';

-- A short ATTACH replays the stored definition, so it is allowed even with the setting off.
DETACH DICTIONARY model_05257_xgb;
SET enable_xgboost = 0;
ATTACH DICTIONARY model_05257_xgb;

SELECT count() FROM system.dictionaries WHERE database = currentDatabase() AND name = 'model_05257_xgb';

DROP DICTIONARY model_05257_xgb;
DROP TABLE training_05257;
