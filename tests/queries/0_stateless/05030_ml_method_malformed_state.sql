-- A state of a machine learning aggregate function is deserialized from the data, so the number of
-- weights it holds must agree both with the gradient it holds and with the number of features of
-- the type; otherwise the prediction reads past the end of the weights.

-- 8 bytes of bias, one weight, iteration number, an empty gradient and the batch size.
SELECT CAST(unhex('00000000000000000100000000000000000000000000000000000000000000000000')
    AS AggregateFunction(stochasticLogisticRegression(0.1, 0, 1, 'SGD'), Float64, Float64, Float64, Float64)); -- { serverError INCORRECT_DATA }

-- The same, with a gradient of two values, so the state itself is consistent, but it declares one
-- weight while the type declares three features.
WITH CAST(unhex('0000000000000000010000000000000000000000000000000002000000000000000000000000000000000000000000000000')
    AS AggregateFunction(stochasticLogisticRegression(0.1, 0, 1, 'SGD'), Float64, Float64, Float64, Float64)) AS state
SELECT evalMLMethod(state, toFloat64(1), toFloat64(1), toFloat64(1)); -- { serverError INCORRECT_DATA }
