---
sidebar_position: 221
---

# stochasticLinearRegression {#agg_functions-stochasticlinearregression}

This function implements stochastic linear regression. It supports custom parameters for learning rate, L2 regularization coefficient, mini-batch size and has few methods for updating weights ([Adam](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Adam) (used by default), [simple SGD](https://en.wikipedia.org/wiki/Stochastic_gradient_descent), [Momentum](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Momentum), [Nesterov](https://mipt.ru/upload/medialibrary/d7e/41-91.pdf)).

### Parameters {#agg_functions-stochasticlinearregression-parameters}

There are 4 customizable parameters. They are passed to the function sequentially, but there is no need to pass all four - default values will be used, however good model required some parameter tuning.

``` text
stochasticLinearRegression(1.0, 1.0, 10, 'SGD')
```

1.  `learning rate` is the coefficient on step length, when gradient descent step is performed. Too big learning rate may cause infinite weights of the model. Default is `0.00001`.
2.  `l2 regularization coefficient` which may help to prevent overfitting. Default is `0.1`.
3.  `mini-batch size` sets the number of elements, which gradients will be computed and summed to perform one step of gradient descent. Pure stochastic descent uses one element, however having small batches(about 10 elements) make gradient steps more stable. Default is `15`.
4.  `method for updating weights`, they are: `Adam` (by default), `SGD`, `Momentum`, `Nesterov`. `Momentum` and `Nesterov` require little bit more computations and memory, however they happen to be useful in terms of speed of convergance and stability of stochastic gradient methods.

### Usage {#agg_functions-stochasticlinearregression-usage}

`stochasticLinearRegression` is used in two steps: fitting the model and predicting on new data. In order to fit the model and save its state for later usage we use `-State` combinator, which basically saves the state (model weights, etc).
To predict we use function [evalMLMethod](../../../sql-reference/functions/machine-learning-functions.md#machine_learning_methods-evalmlmethod), which takes a state as an argument as well as features to predict on.

<a name="stochasticlinearregression-usage-fitting"></a>

**1.** Fitting

Such query may be used.

``` sql
CREATE TABLE IF NOT EXISTS train_data
(
    param1 Float64,
    param2 Float64,
    target Float64
) ENGINE = Memory;

CREATE TABLE your_model ENGINE = Memory AS SELECT
stochasticLinearRegressionState(0.1, 0.0, 5, 'SGD')(target, param1, param2)
AS state FROM train_data;
```

Here we also need to insert data into `train_data` table. The number of parameters is not fixed, it depends only on number of arguments, passed into `linearRegressionState`. They all must be numeric values.
Note that the column with target value(which we would like to learn to predict) is inserted as the first argument.

**2.** Predicting

After saving a state into the table, we may use it multiple times for prediction, or even merge with other states and create new even better models.

``` sql
WITH (SELECT state FROM your_model) AS model SELECT
evalMLMethod(model, param1, param2) FROM test_data
```

The query will return a column of predicted values. Note that first argument of `evalMLMethod` is `AggregateFunctionState` object, next are columns of features.

`test_data` is a table like `train_data` but may not contain target value.

### Notes {#agg_functions-stochasticlinearregression-notes}

1.  To merge two models user may create such query:
    `sql  SELECT state1 + state2 FROM your_models`
    where `your_models` table contains both models. This query will return new `AggregateFunctionState` object.

2.  User may fetch weights of the created model for its own purposes without saving the model if no `-State` combinator is used.
    `sql  SELECT stochasticLinearRegression(0.01)(target, param1, param2) FROM train_data`
    Such query will fit the model and return its weights - first are weights, which correspond to the parameters of the model, the last one is bias. So in the example above the query will return a column with 3 values.

**See Also**

-   [stochasticLogisticRegression](../../../sql-reference/aggregate-functions/reference/stochasticlogisticregression.md#agg_functions-stochasticlogisticregression)
-   [Difference between linear and logistic regressions](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)
