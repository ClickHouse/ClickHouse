-- Tags: no-fasttest, no-parallel-replicas
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.
-- no-parallel-replicas: the dictionary exists only on the initiator, so a query spread over the
-- replicas fails with `Dictionary (model_05054) not found` on the others.

-- An XGBOOST dictionary must actually learn its source data, and `predictXGBoost` must bind its
-- positional arguments to the key columns in declaration order.
--
-- Checking only that a prediction is finite is not enough: that passes even if the features are bound
-- to the wrong arguments, if the target were swapped with a feature, or if the booster returned a
-- constant. The assertions below compare the prediction against the known target instead.

SET enable_xgboost = 1;

DROP DICTIONARY IF EXISTS model_05054;
DROP TABLE IF EXISTS training_05054;

CREATE TABLE training_05054
(
    x1 Float64,
    x2 Float64,
    y Float64
)
ENGINE = MergeTree
ORDER BY tuple();

-- A full 10x10 grid, so the two features are independent and every combination is present: no
-- prediction below has to extrapolate. The coefficients are deliberately asymmetric (1 and 10) so
-- that binding the arguments the wrong way around changes the answer by a lot rather than a little.
INSERT INTO training_05054 (x1, x2, y)
SELECT
    number % 10 AS x1,
    intDiv(number, 10) AS x2,
    x1 + 10 * x2 AS y
FROM numbers(100);

CREATE DICTIONARY model_05054 (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05054'))
LAYOUT(XGBOOST(num_iterations 200 max_depth 6 eta 0.3 seed 42))
LIFETIME(0);

-- The model reproduces the target everywhere on the grid.
SELECT 'The model reproduces its training target';
SELECT max(abs(predictXGBoost('model_05054', x1, x2) - y)) < 1 FROM training_05054;

-- y = x1 + 10 * x2, so y(3, 4) = 43 and y(4, 3) = 34. Asserting both pins the argument order: a
-- prediction that ignored the order could not match the two different expected values.
SELECT 'Features are bound to the arguments in key order';
SELECT abs(predictXGBoost('model_05054', 3.0, 4.0) - 43) < 1;
SELECT abs(predictXGBoost('model_05054', 4.0, 3.0) - 34) < 1;
SELECT predictXGBoost('model_05054', 3.0, 4.0) != predictXGBoost('model_05054', 4.0, 3.0);

DROP DICTIONARY model_05054;
DROP TABLE training_05054;
