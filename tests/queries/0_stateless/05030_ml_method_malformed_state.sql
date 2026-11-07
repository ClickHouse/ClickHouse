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

-- The weights updaters keep their own vectors of the gradient size, so they are validated in the
-- same way. Here the state itself is consistent (one weight, a gradient of two values), but the
-- `Momentum` updater holds a single accumulated gradient value instead of two.
SELECT CAST(unhex('0000000000000000010000000000000000000000000000000002000000000000000000000000000000000000000000000000010000000000000000')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Momentum'), Float64, Float64)); -- { serverError INCORRECT_DATA }

-- The same for `Adam`, which holds two vectors: the average squared gradient is too short.
SELECT CAST(unhex('00000000000000000100000000000000000000000000000000020000000000000000000000000000000000000000000000000200000000000000000000000000000000010000000000000000')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Adam'), Float64, Float64)); -- { serverError INCORRECT_DATA }

-- An empty updater vector is valid: versions before 23.2 serialized the vectors empty until the
-- first update.
SELECT finalizeAggregation(CAST(unhex('000000000000000001000000000000000000000000000000000200000000000000000000000000000000000000000000000000')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Momentum'), Float64, Float64)));

-- The type implies the length of every vector of the state, so a state declaring a longer vector is
-- refused before the vector is allocated. Each state below declares 2^30 values and has none of them;
-- the memory limit makes the query fail differently if the vector is allocated first.
-- The weights.
SELECT CAST(unhex('00000000000000008080808004')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Momentum'), Float64, Float64)) SETTINGS max_memory_usage = 100000000; -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT CAST(unhex('00000000000000008080808004')
    AS AggregateFunction(stochasticLogisticRegression(0.1, 0, 1, 'SGD'), Float64, Float64)) SETTINGS max_memory_usage = 100000000; -- { serverError TOO_LARGE_ARRAY_SIZE }
-- The gradient.
SELECT CAST(unhex('000000000000000001000000000000000000000000000000008080808004')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Momentum'), Float64, Float64)) SETTINGS max_memory_usage = 100000000; -- { serverError TOO_LARGE_ARRAY_SIZE }
-- The vectors of the weights updaters.
SELECT CAST(unhex('00000000000000000100000000000000000000000000000000020000000000000000000000000000000000000000000000008080808004')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Momentum'), Float64, Float64)) SETTINGS max_memory_usage = 100000000; -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT CAST(unhex('00000000000000000100000000000000000000000000000000020000000000000000000000000000000000000000000000008080808004')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Nesterov'), Float64, Float64)) SETTINGS max_memory_usage = 100000000; -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT CAST(unhex('00000000000000000100000000000000000000000000000000020000000000000000000000000000000000000000000000008080808004')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Adam'), Float64, Float64)) SETTINGS max_memory_usage = 100000000; -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT CAST(unhex('000000000000000001000000000000000000000000000000000200000000000000000000000000000000000000000000000002000000000000000000000000000000008080808004')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Adam'), Float64, Float64)) SETTINGS max_memory_usage = 100000000; -- { serverError TOO_LARGE_ARRAY_SIZE }

-- One value more than the type implies is refused in the same way.
SELECT CAST(unhex('000000000000000002')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Momentum'), Float64, Float64)); -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT CAST(unhex('0000000000000000010000000000000000000000000000000003')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Momentum'), Float64, Float64)); -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT CAST(unhex('000000000000000001000000000000000000000000000000000200000000000000000000000000000000000000000000000003')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Momentum'), Float64, Float64)); -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT CAST(unhex('000000000000000001000000000000000000000000000000000200000000000000000000000000000000000000000000000003')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Nesterov'), Float64, Float64)); -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT CAST(unhex('000000000000000001000000000000000000000000000000000200000000000000000000000000000000000000000000000003')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Adam'), Float64, Float64)); -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT CAST(unhex('0000000000000000010000000000000000000000000000000002000000000000000000000000000000000000000000000000020000000000000000000000000000000003')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Adam'), Float64, Float64)); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- A state with exactly the lengths the type implies is still read: bias 2, weight 1.5, non-empty updater vectors.
SELECT finalizeAggregation(CAST(unhex('000000000000004001000000000000F83F01000000000000000200000000000000000000000000000000000000000000000002000000000000D03F000000000000E03F')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Momentum'), Float64, Float64)));
SELECT finalizeAggregation(CAST(unhex('000000000000004001000000000000F83F01000000000000000200000000000000000000000000000000000000000000000002000000000000D03F000000000000E03F')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Nesterov'), Float64, Float64)));
SELECT finalizeAggregation(CAST(unhex('000000000000004001000000000000F83F01000000000000000200000000000000000000000000000000000000000000000002000000000000D03F000000000000E03F02000000000000C03F000000000000E83F')
    AS AggregateFunction(stochasticLinearRegression(0.1, 0, 1, 'Adam'), Float64, Float64)));
