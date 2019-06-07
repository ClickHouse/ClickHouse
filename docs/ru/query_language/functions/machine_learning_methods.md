# Machine learning functions

## evalMLMethod (prediction)

Prediction using fitted regression models uses `evalMLMethod` function. See link in `linearRegression`.

## Stochastic Linear Regression

`stochasticLinearRegression` aggregate function implements stochastic gradient descent method using linear model and MSE loss function. Uses `evalMLMethod` to predict on new data.
See examples and notes [here](../agg_functions/reference.md#agg_functions-linearregression).

## Stochastic Logistic Regression

`stochasticLogisticRegression` aggregate function implements stochastic gradient descent method for binary classification problem. Uses `evalMLMethod` to predict on new data.
See examples and notes [here](../agg_functions/reference.md#agg_functions-logisticregression).
