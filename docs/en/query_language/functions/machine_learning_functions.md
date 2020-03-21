# Machine learning functions {#machine-learning-functions}

## evalMLMethod (prediction) {#machine-learning-methods-evalmlmethod}

Prediction using fitted regression models uses `evalMLMethod` function. See link in `linearRegression`.

### Stochastic Linear Regression {#stochastic-linear-regression}

The [stochasticLinearRegression](../agg_functions/reference.md#agg_functions-stochasticlinearregression) aggregate function implements stochastic gradient descent method using linear model and MSE loss function. Uses `evalMLMethod` to predict on new data.

### Stochastic Logistic Regression {#stochastic-logistic-regression}

The [stochasticLogisticRegression](../agg_functions/reference.md#agg_functions-stochasticlogisticregression) aggregate function implements stochastic gradient descent method for binary classification problem. Uses `evalMLMethod` to predict on new data.
