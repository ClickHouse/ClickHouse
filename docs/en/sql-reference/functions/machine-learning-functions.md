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

## bayesAB {#bayesab}

Compares test groups (variants) and calculates for each group the probability to be the best one. The first group is used as a control group.

**Syntax** 

``` sql
bayesAB(distribution_name, higher_is_better, variant_names, x, y)
```

**Parameters** 

-   `distribution_name` — Name of the probability distribution. [String](../../sql-reference/data-types/string.md). Possible values:

    -   `beta` for [Beta distribution](https://en.wikipedia.org/wiki/Beta_distribution)
    -   `gamma` for [Gamma distribution](https://en.wikipedia.org/wiki/Gamma_distribution)

-   `higher_is_better` — Boolean flag. [Boolean](../../sql-reference/data-types/boolean.md). Possible values:

    -    `0` - lower values are considered to be better than higher
    -    `1` - higher values are considered to be better than lower

-   `variant_names` - Variant names. [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

-   `x` - Numbers of tests for the corresponding variants. [Array](../../sql-reference/data-types/array.md)([Float64](../../sql-reference/data-types/float.md)).

-   `y` - Numbers of successful tests for the corresponding variants. [Array](../../sql-reference/data-types/array.md)([Float64](../../sql-reference/data-types/float.md)).

!!! note "Note"
    All three arrays must have the same size. All `x` and `y` values must be non-negative constant numbers. `y` cannot be larger than `x`.

**Returned values**

For each variant the function calculates:
-   `beats_control` - long-term probability to out-perform the first (control) variant
-   `to_be_best` - long-term probability to out-perform all other variants

Type: JSON.

**Example**

Query:

``` sql
SELECT bayesAB('beta', 1, ['Control', 'A', 'B'], [3000., 3000., 3000.], [100., 90., 110.]) FORMAT PrettySpace;
```

Result:

``` text
{
   "data":[
      {
         "variant_name":"Control",
         "x":3000,
         "y":100,
         "beats_control":0,
         "to_be_best":0.22619
      },
      {
         "variant_name":"A",
         "x":3000,
         "y":90,
         "beats_control":0.23469,
         "to_be_best":0.04671
      },
      {
         "variant_name":"B",
         "x":3000,
         "y":110,
         "beats_control":0.7580899999999999,
         "to_be_best":0.7271
      }
   ]
}
```

[Original article](https://clickhouse.tech/docs/en/query_language/functions/machine-learning-functions/) <!--hide-->
