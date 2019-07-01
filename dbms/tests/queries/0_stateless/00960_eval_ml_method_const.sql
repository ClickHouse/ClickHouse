WITH (SELECT stochasticLinearRegressionState(1, 2, 3)) AS model SELECT evalMLMethod(model, toFloat64(1), toFloat64(1));
