---
toc_priority: 221
---

# stochasticLinearRegression {#agg_functions-stochasticlinearregression}

该函数实现随机线性回归。 它支持自定义参数的学习率、L2正则化系数、微批，并且具有少量更新权重的方法（[Adam](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Adam) （默认）， [simple SGD](https://en.wikipedia.org/wiki/Stochastic_gradient_descent)， [Momentum](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Momentum)， [Nesterov](https://mipt.ru/upload/medialibrary/d7e/41-91.pdf)）。

### 参数 {#agg_functions-stochasticlinearregression-parameters}

有4个可自定义的参数。它们按顺序传递给函数，但不需要传递所有四个参数——将使用默认值，然而好的模型需要一些参数调整。

**语法**

``` sql
stochasticLinearRegression(1.0, 1.0, 10, 'SGD')
```

1.  `learning rate` 当执行梯度下降步骤时，步长的系数。 过大的学习率可能会导致模型的权重无限大。 默认值为 `0.00001`。
2.  `l2 regularization coefficient` 这可能有助于防止过度拟合。 默认值为 `0.1`。
3.  `mini-batch size` 设置元素的数量，这些元素将被计算和求和以执行梯度下降的一个步骤。纯随机下降使用一个元素，但是具有小批量（约10个元素）使梯度步骤更稳定。 默认值为 `15`。
4.  `method for updating weights` 他们是: `Adam` (默认情况下), `SGD`, `Momentum`, `Nesterov`。`Momentum` 和 `Nesterov` 需要更多的计算和内存，但是它们恰好在收敛速度和随机梯度方法的稳定性方面是有用的。

### 使用 {#agg_functions-stochasticlinearregression-usage}

`stochasticLinearRegression` 用于两个步骤：拟合模型和预测新数据。 为了拟合模型并保存其状态以供以后使用，我们使用 `-State` 组合器，它基本上保存了状态（模型权重等）。
为了预测我们使用函数 [evalMLMethod](../../../sql-reference/functions/machine-learning-functions.md#machine_learning_methods-evalmlmethod), 这需要一个状态作为参数以及特征来预测。

<a name="stochasticlinearregression-usage-fitting"></a>

**1.** 拟合

可以使用这种查询。

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

在这里，我们还需要将数据插入到 `train_data` 表。参数的数量不是固定的，它只取决于传入 `linearRegressionState` 的参数数量。它们都必须是数值。
注意，目标值(我们想学习预测的)列作为第一个参数插入。

**2.** 预测

在将状态保存到表中之后，我们可以多次使用它进行预测，甚至与其他状态合并，创建新的更好的模型。

``` sql
WITH (SELECT state FROM your_model) AS model SELECT
evalMLMethod(model, param1, param2) FROM test_data
```

查询将返回一列预测值。注意，`evalMLMethod` 的第一个参数是 `AggregateFunctionState` 对象, 接下来是特征列。

`test_data` 是一个类似 `train_data` 的表 但可能不包含目标值。

### 注 {#agg_functions-stochasticlinearregression-notes}

1.  要合并两个模型，用户可以创建这样的查询:
    `sql  SELECT state1 + state2 FROM your_models`
    其中 `your_models` 表包含这两个模型。此查询将返回新的 `AggregateFunctionState` 对象。

2.  如果没有使用 `-State` 组合器，用户可以为自己的目的获取所创建模型的权重，而不保存模型 。
    `sql  SELECT stochasticLinearRegression(0.01)(target, param1, param2) FROM train_data`
    这样的查询将拟合模型，并返回其权重——首先是权重，对应模型的参数，最后一个是偏差。 所以在上面的例子中，查询将返回一个具有3个值的列。

**参见**

-   [随机指标逻辑回归](../../../sql-reference/aggregate-functions/reference/stochasticlogisticregression.md#agg_functions-stochasticlogisticregression)
-   [线性回归和逻辑回归之间的差异](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)
