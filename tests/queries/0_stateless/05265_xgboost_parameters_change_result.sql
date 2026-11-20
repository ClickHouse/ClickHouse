-- Tags: no-fasttest
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.

-- The training parameters of `LAYOUT(XGBOOST(...))` and the prediction parameters of `predictXGBoost` reach
-- XGBoost: each of them changes the prediction, rather than only being accepted.

SET enable_xgboost = 1;

DROP TABLE IF EXISTS training_05265;
DROP TABLE IF EXISTS training_binary_05265;

CREATE TABLE training_05265 (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO training_05265 SELECT number AS x1, intDiv(number, 7) AS x2, 2 * x1 + 3 * x2 AS y FROM numbers(100);

SELECT 'Training parameters';

-- A baseline, and one dictionary per parameter that differs from it in that parameter only.
DROP DICTIONARY IF EXISTS model_05265_base;
CREATE DICTIONARY model_05265_base (x1 Float64, x2 Float64, y Float64) PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05265')) LAYOUT(XGBOOST(num_iterations 10 max_depth 3)) LIFETIME(0);

DROP DICTIONARY IF EXISTS model_05265_num_iterations;
CREATE DICTIONARY model_05265_num_iterations (x1 Float64, x2 Float64, y Float64) PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05265')) LAYOUT(XGBOOST(num_iterations 20 max_depth 3)) LIFETIME(0);

DROP DICTIONARY IF EXISTS model_05265_eta;
CREATE DICTIONARY model_05265_eta (x1 Float64, x2 Float64, y Float64) PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05265')) LAYOUT(XGBOOST(num_iterations 10 max_depth 3 eta 0.1)) LIFETIME(0);

DROP DICTIONARY IF EXISTS model_05265_lambda;
CREATE DICTIONARY model_05265_lambda (x1 Float64, x2 Float64, y Float64) PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05265')) LAYOUT(XGBOOST(num_iterations 10 max_depth 3 lambda 50)) LIFETIME(0);

DROP DICTIONARY IF EXISTS model_05265_objective;
CREATE DICTIONARY model_05265_objective (x1 Float64, x2 Float64, y Float64) PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05265')) LAYOUT(XGBOOST(num_iterations 10 max_depth 3 objective 'reg:absoluteerror')) LIFETIME(0);

DROP DICTIONARY IF EXISTS model_05265_min_child_weight;
CREATE DICTIONARY model_05265_min_child_weight (x1 Float64, x2 Float64, y Float64) PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05265')) LAYOUT(XGBOOST(num_iterations 10 max_depth 3 min_child_weight 40)) LIFETIME(0);

SELECT 'num_iterations', predictXGBoost('model_05265_num_iterations', 30.0, 4.0) != predictXGBoost('model_05265_base', 30.0, 4.0);
SELECT 'eta', predictXGBoost('model_05265_eta', 30.0, 4.0) != predictXGBoost('model_05265_base', 30.0, 4.0);
SELECT 'lambda', predictXGBoost('model_05265_lambda', 30.0, 4.0) != predictXGBoost('model_05265_base', 30.0, 4.0);
SELECT 'objective', predictXGBoost('model_05265_objective', 30.0, 4.0) != predictXGBoost('model_05265_base', 30.0, 4.0);
SELECT 'min_child_weight', predictXGBoost('model_05265_min_child_weight', 30.0, 4.0) != predictXGBoost('model_05265_base', 30.0, 4.0);

-- The same parameters train the same model, so the differences above come from the parameters.
DROP DICTIONARY IF EXISTS model_05265_base_again;
CREATE DICTIONARY model_05265_base_again (x1 Float64, x2 Float64, y Float64) PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05265')) LAYOUT(XGBOOST(num_iterations 10 max_depth 3)) LIFETIME(0);
SELECT predictXGBoost('model_05265_base_again', 30.0, 4.0) = predictXGBoost('model_05265_base', 30.0, 4.0);

SELECT 'Prediction parameters: iteration_begin and iteration_end';

-- All 10 rounds, given explicitly, are the full model; fewer rounds or a later start are not.
SELECT predictXGBoost('model_05265_base', 30.0, 4.0, map('iteration_begin', 0, 'iteration_end', 10)) = predictXGBoost('model_05265_base', 30.0, 4.0);
SELECT predictXGBoost('model_05265_base', 30.0, 4.0, map('iteration_end', 1)) != predictXGBoost('model_05265_base', 30.0, 4.0);
SELECT predictXGBoost('model_05265_base', 30.0, 4.0, map('iteration_begin', 5)) != predictXGBoost('model_05265_base', 30.0, 4.0);
-- More rounds fit the training data more closely: y(30, 4) = 72.
SELECT abs(predictXGBoost('model_05265_base', 30.0, 4.0, map('iteration_end', 1)) - 72) > abs(predictXGBoost('model_05265_base', 30.0, 4.0) - 72);

SELECT 'Prediction parameters: type';

-- With `binary:logistic`, `type 0` is the probability and `type 1` the margin (log-odds) it is computed from.
CREATE TABLE training_binary_05265 (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO training_binary_05265 SELECT number AS x1, intDiv(number, 7) AS x2, x1 > 50 AS y FROM numbers(100);

DROP DICTIONARY IF EXISTS model_05265_binary;
CREATE DICTIONARY model_05265_binary (x1 Float64, x2 Float64, y Float64) PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_binary_05265')) LAYOUT(XGBOOST(num_iterations 10 max_depth 3 objective 'binary:logistic')) LIFETIME(0);

WITH
    predictXGBoost('model_05265_binary', x1, 4.0, map('type', 0)) AS probability,
    predictXGBoost('model_05265_binary', x1, 4.0, map('type', 1)) AS margin
SELECT x1, probability BETWEEN 0 AND 1, probability != margin, abs(probability - 1 / (1 + exp(-margin))) < 1e-6
FROM (SELECT arrayJoin([10.0, 80.0]) AS x1)
ORDER BY x1;

DROP DICTIONARY model_05265_base;
DROP DICTIONARY model_05265_base_again;
DROP DICTIONARY model_05265_num_iterations;
DROP DICTIONARY model_05265_eta;
DROP DICTIONARY model_05265_lambda;
DROP DICTIONARY model_05265_objective;
DROP DICTIONARY model_05265_min_child_weight;
DROP DICTIONARY model_05265_binary;
DROP TABLE training_05265;
DROP TABLE training_binary_05265;
