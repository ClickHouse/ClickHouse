---
toc_priority: 64
toc_title: Machine Learning
---

# Machine Learning Functions {#machine-learning-functions}

## evalMLMethod {#machine_learning_methods-evalmlmethod}

Prediction using fitted regression models uses `evalMLMethod` function. See link in `linearRegression`.

## stochasticLinearRegressionn {#stochastic-linear-regression}

The [stochasticLinearRegression](../../sql-reference/aggregate-functions/reference/stochasticlinearregression.md#agg_functions-stochasticlinearregression) aggregate function implements stochastic gradient descent method using linear model and MSE loss function. Uses `evalMLMethod` to predict on new data.

## stochasticLogisticRegression {#stochastic-logistic-regression}

The [stochasticLogisticRegression](../../sql-reference/aggregate-functions/reference/stochasticlogisticregression.md#agg_functions-stochasticlogisticregression) aggregate function implements stochastic gradient descent method for binary classification problem. Uses `evalMLMethod` to predict on new data.
