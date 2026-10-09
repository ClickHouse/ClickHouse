-- Tags: no-fasttest
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.

-- A NULL feature is passed to XGBoost as a missing value (NaN) rather than turning the prediction into NULL,
-- so `predictXGBoost` always returns `Float64`. With every feature constant, the result is a constant.

SET enable_xgboost = 1;

DROP DICTIONARY IF EXISTS model_05262_xgb;
DROP TABLE IF EXISTS training_05262;

CREATE TABLE training_05262 (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO training_05262 SELECT number AS x1, intDiv(number, 7) AS x2, 2 * x1 + 3 * x2 AS y FROM numbers(100);

CREATE DICTIONARY model_05262_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05262'))
LAYOUT(XGBOOST(num_iterations 10))
LIFETIME(0);

SELECT 'The result type is Float64, also for Nullable features';
SELECT toTypeName(predictXGBoost('model_05262_xgb', 1.0, 2.0));
SELECT toTypeName(predictXGBoost('model_05262_xgb', toNullable(1.0), 2.0));
SELECT toTypeName(predictXGBoost('model_05262_xgb', NULL, 2.0));

SELECT 'A NULL feature is predicted like a NaN feature, i.e. as a missing value';
SELECT predictXGBoost('model_05262_xgb', x, 2.0) = predictXGBoost('model_05262_xgb', nan, 2.0)
FROM (SELECT arrayJoin([NULL, NULL]::Array(Nullable(Float64))) AS x);
SELECT predictXGBoost('model_05262_xgb', NULL, 2.0) = predictXGBoost('model_05262_xgb', nan, 2.0);

SELECT 'A non-NULL value of a Nullable feature is predicted like the plain value';
SELECT predictXGBoost('model_05262_xgb', x, 2.0) = predictXGBoost('model_05262_xgb', assumeNotNull(x), 2.0)
FROM (SELECT arrayJoin([1.0, 50.0, NULL]::Array(Nullable(Float64))) AS x)
WHERE x IS NOT NULL;

SELECT 'Constant features give a constant result, equal to the per-row prediction';
SELECT isConstant(predictXGBoost('model_05262_xgb', 1.0, 2.0));
SELECT isConstant(predictXGBoost('model_05262_xgb', materialize(1.0), 2.0));
SELECT countIf(predictXGBoost('model_05262_xgb', 1.0, 2.0) = predictXGBoost('model_05262_xgb', materialize(1.0), materialize(2.0)))
FROM numbers(1000);

SELECT 'Prediction parameters work with constant and non-constant features, and must be constant';
SELECT isConstant(predictXGBoost('model_05262_xgb', 1.0, 2.0, map('iteration_end', 5)));
SELECT predictXGBoost('model_05262_xgb', 1.0, 2.0, map('iteration_end', 5)) = predictXGBoost('model_05262_xgb', materialize(1.0), 2.0, map('iteration_end', 5));
SELECT predictXGBoost('model_05262_xgb', 1.0, 2.0, map('iteration_end', 5)) != predictXGBoost('model_05262_xgb', 1.0, 2.0);
SELECT predictXGBoost('model_05262_xgb', 1.0, 2.0, materialize(map('iteration_end', 5))); -- { serverError ILLEGAL_COLUMN }
SELECT predictXGBoost('model_05262_xgb', materialize(1.0), 2.0, materialize(map('iteration_end', 5))); -- { serverError ILLEGAL_COLUMN }

SELECT 'A non-numeric feature is still rejected';
SELECT predictXGBoost('model_05262_xgb', 'a', 2.0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT predictXGBoost('model_05262_xgb', toNullable('a'), 2.0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP DICTIONARY model_05262_xgb;
DROP TABLE training_05262;
