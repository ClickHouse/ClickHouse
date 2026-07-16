-- Regression test: a model of stochasticLinearRegression / stochasticLogisticRegression trained over a Variant
-- argument (via AggregateFunctionVariantAdapter, and the Null combinator it delegates to for the Nullable(Float64)
-- fallback supertype) must still be usable with evalMLMethod: both getReturnTypeToPredict and predictValues must be
-- forwarded all the way down to the nested AggregateFunctionMLMethod state.

SET allow_experimental_variant_type = 1;
-- Single-threaded aggregation so that the SGD mini-batch boundaries (and therefore the trained weights) are
-- deterministic and identical between the plain and the Variant trainings.
SET max_threads = 1;

DROP TABLE IF EXISTS t_variant_agg_ml;
CREATE TABLE t_variant_agg_ml (target Float64, f1 Float64, f2 Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_variant_agg_ml SELECT 3 * number + 5 + 0.1 * (number % 7), number, 2 * number FROM numbers(100);

-- A model trained over Variant(Int64, Float64) arguments predicts, and predicts exactly the same values as the
-- model trained over the same values with their plain types (the adapter casts the arguments to the Float64
-- fallback supertype, and the values are identical).
SELECT 'linear over Variant';
WITH
    (SELECT stochasticLinearRegressionState(0.001, 0.1, 5, 'SGD')(target, f1, f2) FROM t_variant_agg_ml) AS plain_model,
    (SELECT stochasticLinearRegressionState(0.001, 0.1, 5, 'SGD')(
        CAST(target, 'Variant(Int64, Float64)'),
        CAST(f1, 'Variant(Int64, Float64)'),
        CAST(f2, 'Variant(Int64, Float64)')) FROM t_variant_agg_ml) AS variant_model
SELECT
    toTypeName(variant_model),
    isFinite(evalMLMethod(variant_model, 10.0, 20.0)),
    evalMLMethod(variant_model, 10.0, 20.0) = evalMLMethod(plain_model, 10.0, 20.0),
    evalMLMethod(variant_model, 42.0, 84.0) = evalMLMethod(plain_model, 42.0, 84.0);

SELECT 'logistic over Variant';
WITH
    (SELECT stochasticLogisticRegressionState(0.001, 0.1, 5, 'SGD')(toFloat64(target > 150), f1, f2) FROM t_variant_agg_ml) AS plain_model,
    (SELECT stochasticLogisticRegressionState(0.001, 0.1, 5, 'SGD')(
        CAST(toFloat64(target > 150), 'Variant(Int64, Float64)'),
        CAST(f1, 'Variant(Int64, Float64)'),
        CAST(f2, 'Variant(Int64, Float64)')) FROM t_variant_agg_ml) AS variant_model
SELECT
    toTypeName(variant_model),
    evalMLMethod(variant_model, 10.0, 20.0) BETWEEN 0 AND 1,
    evalMLMethod(variant_model, 10.0, 20.0) = evalMLMethod(plain_model, 10.0, 20.0);

-- The implicit NULLs of a Variant are skipped during the training, exactly as ordinary NULLs are: a model trained
-- over a Variant column with NULL rows equals the model trained over the plain non-NULL subset.
SELECT 'linear over Variant with NULLs';
WITH
    (SELECT stochasticLinearRegressionState(0.001, 0.1, 5, 'SGD')(target, f1, f2) FROM t_variant_agg_ml WHERE f1 < 50) AS plain_model,
    (SELECT stochasticLinearRegressionState(0.001, 0.1, 5, 'SGD')(
        CAST(if(f1 < 50, target, NULL), 'Variant(Int64, Float64)'),
        CAST(f1, 'Variant(Int64, Float64)'),
        CAST(f2, 'Variant(Int64, Float64)')) FROM t_variant_agg_ml) AS variant_model
SELECT evalMLMethod(variant_model, 10.0, 20.0) = evalMLMethod(plain_model, 10.0, 20.0);

-- The same model trained over plain Nullable arguments (the case the Variant adapter reduces to) also predicts:
-- the Null combinator wrapping AggregateFunctionMLMethod must forward the prediction hooks too.
SELECT 'linear over Nullable';
WITH
    (SELECT stochasticLinearRegressionState(0.001, 0.1, 5, 'SGD')(target, f1, f2) FROM t_variant_agg_ml) AS plain_model,
    (SELECT stochasticLinearRegressionState(0.001, 0.1, 5, 'SGD')(toNullable(target), toNullable(f1), toNullable(f2)) FROM t_variant_agg_ml) AS nullable_model
SELECT
    toTypeName(nullable_model),
    evalMLMethod(nullable_model, 10.0, 20.0) = evalMLMethod(plain_model, 10.0, 20.0);

DROP TABLE t_variant_agg_ml;
