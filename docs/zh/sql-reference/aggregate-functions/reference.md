---
toc_priority: 36
toc_title: 参考手册
---

# 参考手册 {#aggregate-functions-reference}


## quantileExact {#quantileexact}

准确计算数字序列的[分位数](https://en.wikipedia.org/wiki/Quantile)。

为了准确计算，所有输入的数据被合并为一个数组，并且部分的排序。因此该函数需要 `O(n)` 的内存，n为输入数据的个数。但是对于少量数据来说，该函数还是非常有效的。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用[分位数](#quantiles)功能。

**语法**

``` sql
quantileExact(level)(expr)
```

别名: `medianExact`.

**参数**

-   `level` — 分位数层次。可选参数。 从0到1的一个float类型的常量。 我们推荐 `level` 值的范围为 `[0.01, 0.99]`. 默认值：0.5。 在 `level=0.5` 该函数计算 [中位数](https://en.wikipedia.org/wiki/Median).
-   `expr` — 求职表达式，类型为：数值[数据类型](../../sql-reference/data-types/index.md#data_types),[日期](../../sql-reference/data-types/date.md)数据类型或[时间](../../sql-reference/data-types/datetime.md)数据类型。

**返回值**

-   指定层次的分位数。

类型:

-   [Float64](../../sql-reference/data-types/float.md) 对于数字数据类型输入。
-   [日期](../../sql-reference/data-types/date.md) 如果输入值具有 `Date` 类型。
-   [日期时间](../../sql-reference/data-types/datetime.md) 如果输入值具有 `DateTime` 类型。

**示例**

查询:

``` sql
SELECT quantileExact(number) FROM numbers(10)
```

结果:

``` text
┌─quantileExact(number)─┐
│                     5 │
└───────────────────────┘
```

**另请参阅**

-   [中位数](#median)
-   [分位数](#quantiles)

## quantileExactWeighted {#quantileexactweighted}

考虑到每个元素的权重，然后准确计算数值序列的[分位数](https://en.wikipedia.org/wiki/Quantile)。

为了准确计算，所有输入的数据被合并为一个数组，并且部分的排序。每个输入值需要根据 `weight` 计算求和。该算法使用哈希表。正因为如此，在数据重复较多的时候使用的内存是少于[quantileExact](#quantileexact)的。 您可以使用此函数代替 `quantileExact` 并指定重量1。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用[分位数](#quantiles)功能。

**语法**

``` sql
quantileExactWeighted(level)(expr, weight)
```

别名: `medianExactWeighted`.

**参数**

-   `level` — 分位数层次。可选参数。 从0到1的一个float类型的常量。 我们推荐 `level` 值的范围为 `[0.01, 0.99]`. 默认值：0.5。 在 `level=0.5` 该函数计算 [中位数](https://en.wikipedia.org/wiki/Median).
-   `expr` — 求职表达式，类型为：数值[数据类型](../../sql-reference/data-types/index.md#data_types),[日期](../../sql-reference/data-types/date.md)数据类型或[时间](../../sql-reference/data-types/datetime.md)数据类型。
-   `weight` — 权重序列。 权重是一个数据出现的数值。

**返回值**

-   指定层次的分位数。

类型:

-   [Float64](../../sql-reference/data-types/float.md) 对于数字数据类型输入。
-   [日期](../../sql-reference/data-types/date.md) 如果输入值具有 `Date` 类型。
-   [日期时间](../../sql-reference/data-types/datetime.md) 如果输入值具有 `DateTime` 类型。

**示例**

输入表:

``` text
┌─n─┬─val─┐
│ 0 │   3 │
│ 1 │   2 │
│ 2 │   1 │
│ 5 │   4 │
└───┴─────┘
```

查询:

``` sql
SELECT quantileExactWeighted(n, val) FROM t
```

结果:

``` text
┌─quantileExactWeighted(n, val)─┐
│                             1 │
└───────────────────────────────┘
```

**另请参阅**

-   [中位数](#median)
-   [分位数](#quantiles)

## quantileTiming {#quantiletiming}

使用确定的精度计算数字数据序列的[分位数](https://en.wikipedia.org/wiki/Quantile)。

结果是确定性的（它不依赖于查询处理顺序）。 该函数针对描述加载网页时间或后端响应时间等分布的序列进行了优化。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用[分位数](#quantiles)功能。

**语法**

``` sql
quantileTiming(level)(expr)
```

别名: `medianTiming`.

**参数**

-   `level` — 分位数层次。可选参数。 从0到1的一个float类型的常量。 我们推荐 `level` 值的范围为 `[0.01, 0.99]`. 默认值：0.5。 在 `level=0.5` 该函数计算 [中位数](https://en.wikipedia.org/wiki/Median).

-   `expr` — [表达式](../syntax.md#syntax-expressions)，返回 [浮动\*](../../sql-reference/data-types/float.md)类型数据。

        - 如果输入负值，那结果是不可预期的。
        - 如果输入值大于30000（页面加载时间大于30s），那我们假设为30000。

**精度**

计算是准确的，如果:

-   值的总数不超过5670。
-   总数值超过5670，但页面加载时间小于1024ms。

否则，计算结果将四舍五入到16毫秒的最接近倍数。

!!! note "注"
    对于计算页面加载时间分位数，此函数比 [分位数](#quantile)更有效和准确。

**返回值**

-   指定层次的分位数。

类型: `Float32`.

!!! note "注"
    如果没有值传递给函数（当使用 `quantileTimingIf`), [NaN](../../sql-reference/data-types/float.md#data_type-float-nan-inf) 被返回。 这样做的目的是将这些案例与导致零的案例区分开来。 看 [ORDER BY clause](../statements/select/order-by.md#select-order-by) 对于 `NaN` 值排序注意事项。

**示例**

输入表:

``` text
┌─response_time─┐
│            72 │
│           112 │
│           126 │
│           145 │
│           104 │
│           242 │
│           313 │
│           168 │
│           108 │
└───────────────┘
```

查询:

``` sql
SELECT quantileTiming(response_time) FROM t
```

结果:

``` text
┌─quantileTiming(response_time)─┐
│                           126 │
└───────────────────────────────┘
```

**另请参阅**

-   [中位数](#median)
-   [分位数](#quantiles)

## quantileTimingWeighted {#quantiletimingweighted}

根据每个序列成员的权重，使用确定的精度计算数字序列的[分位数](https://en.wikipedia.org/wiki/Quantile)。

结果是确定性的（它不依赖于查询处理顺序）。 该函数针对描述加载网页时间或后端响应时间等分布的序列进行了优化。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用[分位数](#quantiles)功能。

**语法**

``` sql
quantileTimingWeighted(level)(expr, weight)
```

别名: `medianTimingWeighted`.

**参数**

-   `level` — 分位数层次。可选参数。 从0到1的一个float类型的常量。 我们推荐 `level` 值的范围为 `[0.01, 0.99]`. 默认值：0.5。 在 `level=0.5` 该函数计算 [中位数](https://en.wikipedia.org/wiki/Median).

-   `expr` — [表达式](../syntax.md#syntax-expressions)，返回 [浮动\*](../../sql-reference/data-types/float.md)类型数据。

        - 如果输入负值，那结果是不可预期的。
        - 如果输入值大于30000（页面加载时间大于30s），那我们假设为30000。

-   `weight` — 权重序列。 权重是一个数据出现的数值。

**精度**

计算是准确的，如果:

-   值的总数不超过5670。
-   总数值超过5670，但页面加载时间小于1024ms。

否则，计算结果将四舍五入到16毫秒的最接近倍数。

!!! note "注"
    对于计算页面加载时间分位数，此函数比 [分位数](#quantile)更高效和准确。

**返回值**

-   指定层次的分位数。

类型: `Float32`.

!!! note "注"
    如果没有值传递给函数（当使用 `quantileTimingIf`), [NaN](../../sql-reference/data-types/float.md#data_type-float-nan-inf) 被返回。 这样做的目的是将这些案例与导致零的案例区分开来。看 [ORDER BY clause](../statements/select/order-by.md#select-order-by) 对于 `NaN` 值排序注意事项。

**示例**

输入表:

``` text
┌─response_time─┬─weight─┐
│            68 │      1 │
│           104 │      2 │
│           112 │      3 │
│           126 │      2 │
│           138 │      1 │
│           162 │      1 │
└───────────────┴────────┘
```

查询:

``` sql
SELECT quantileTimingWeighted(response_time, weight) FROM t
```

结果:

``` text
┌─quantileTimingWeighted(response_time, weight)─┐
│                                           112 │
└───────────────────────────────────────────────┘
```

**另请参阅**

-   [中位数](#median)
-   [分位数](#quantiles)

## quantileTDigest {#quantiletdigest}

使用[t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) 算法计算近似[分位数](https://en.wikipedia.org/wiki/Quantile)。

最大误差为1%。 内存消耗 `log(n)`，这里 `n` 是值的个数。 结果取决于运行查询的顺序，并且是不确定的。

该功能的性能低于性能 [分位数](#quantile) 或 [时间分位](#quantiletiming). 在状态大小与精度的比率方面，这个函数比 `quantile`更优秀。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用[分位数](#quantiles)功能。

**语法**

``` sql
quantileTDigest(level)(expr)
```

别名: `medianTDigest`.

**参数**

-   `level` — 分位数层次。可选参数。 从0到1的一个float类型的常量。 我们推荐 `level` 值的范围为 `[0.01, 0.99]`. 默认值：0.5。 在 `level=0.5` 该函数计算 [中位数](https://en.wikipedia.org/wiki/Median).
-   `expr` — 求职表达式，类型为：数值[数据类型](../../sql-reference/data-types/index.md#data_types),[日期](../../sql-reference/data-types/date.md)数据类型或[时间](../../sql-reference/data-types/datetime.md)数据类型。

**回值**

-   指定层次的分位数。

类型:

-   [Float64](../../sql-reference/data-types/float.md) 对于数字数据类型输入。
-   [日期](../../sql-reference/data-types/date.md) 如果输入值具有 `Date` 类型。
-   [日期时间](../../sql-reference/data-types/datetime.md) 如果输入值具有 `DateTime` 类型。

**示例**

查询:

``` sql
SELECT quantileTDigest(number) FROM numbers(10)
```

结果:

``` text
┌─quantileTDigest(number)─┐
│                     4.5 │
└─────────────────────────┘
```

**另请参阅**

-   [中位数](#median)
-   [分位数](#quantiles)

## quantileTDigestWeighted {#quantiletdigestweighted}

使用[t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) 算法计算近似[分位数](https://en.wikipedia.org/wiki/Quantile)。 该函数考虑了每个序列成员的权重。最大误差为1%。 内存消耗 `log(n)`，这里 `n` 是值的个数。

该功能的性能低于性能 [分位数](#quantile) 或 [时间分位](#quantiletiming). 在状态大小与精度的比率方面，这个函数比 `quantile`更优秀。

结果取决于运行查询的顺序，并且是不确定的。

当在一个查询中使用多个不同层次的 `quantile*` 时，内部状态不会被组合（即查询的工作效率低于组合情况）。在这种情况下，使用[分位数](#quantiles)功能

**语法**

``` sql
quantileTDigest(level)(expr)
```

别名: `medianTDigest`.

**参数**

-   `level` — 分位数层次。可选参数。 从0到1的一个float类型的常量。 我们推荐 `level` 值的范围为 `[0.01, 0.99]`. 默认值：0.5。 在 `level=0.5` 该函数计算 [中位数](https://en.wikipedia.org/wiki/Median).
-   `expr` — 求职表达式，类型为：数值[数据类型](../../sql-reference/data-types/index.md#data_types),[日期](../../sql-reference/data-types/date.md)数据类型或[时间](../../sql-reference/data-types/datetime.md)数据类型。
-   `weight` — 权重序列。 权重是一个数据出现的数值。

**返回值**

-   指定层次的分位数。

类型:

-   [Float64](../../sql-reference/data-types/float.md) 对于数字数据类型输入。
-   [日期](../../sql-reference/data-types/date.md) 如果输入值具有 `Date` 类型。
-   [日期时间](../../sql-reference/data-types/datetime.md) 如果输入值具有 `DateTime` 类型。

**示例**

查询:

``` sql
SELECT quantileTDigestWeighted(number, 1) FROM numbers(10)
```

结果:

``` text
┌─quantileTDigestWeighted(number, 1)─┐
│                                4.5 │
└────────────────────────────────────┘
```

**另请参阅**

-   [中位数](#median)
-   [分位数](#quantiles)

## quantiles(level1, level2, …)(x) {#quantiles}

所有分位数函数也有相应的函数: `quantiles`, `quantilesDeterministic`, `quantilesTiming`, `quantilesTimingWeighted`, `quantilesExact`, `quantilesExactWeighted`, `quantilesTDigest`。这些函数一次计算所列层次的所有分位数，并返回结果值的数组。


## stochasticLinearRegression {#agg_functions-stochasticlinearregression}

该函数实现随机线性回归。 它支持自定义参数的学习率、L2正则化系数、微批，并且具有少量更新权重的方法（[Adam](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Adam) （默认）， [simple SGD](https://en.wikipedia.org/wiki/Stochastic_gradient_descent)， [Momentum](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Momentum)， [Nesterov](https://mipt.ru/upload/medialibrary/d7e/41-91.pdf)）。

### 参数 {#agg_functions-stochasticlinearregression-parameters}

有4个可自定义的参数。 它们按顺序传递给函数，但是没有必要传递所有四个默认值将被使用，但是好的模型需要一些参数调整。

``` text
stochasticLinearRegression(1.0, 1.0, 10, 'SGD')
```

1.  `learning rate` 当执行梯度下降步骤时，步长上的系数。 过大的学习率可能会导致模型的权重无限大。 默认值为 `0.00001`.
2.  `l2 regularization coefficient` 这可能有助于防止过度拟合。 默认值为 `0.1`.
3.  `mini-batch size` 设置元素的数量，这些元素将被计算和求和以执行梯度下降的一个步骤。 纯随机下降使用一个元素，但是具有小批量（约10个元素）使梯度步骤更稳定。 默认值为 `15`.
4.  `method for updating weights` 他们是: `Adam` （默认情况下), `SGD`, `Momentum`, `Nesterov`. `Momentum` 和 `Nesterov` 需要更多的计算和内存，但是它们恰好在收敛速度和随机梯度方法的稳定性方面是有用的。

### 用法 {#agg_functions-stochasticlinearregression-usage}

`stochasticLinearRegression` 用于两个步骤：拟合模型和预测新数据。 为了拟合模型并保存其状态以供以后使用，我们使用 `-State` combinator，它基本上保存了状态（模型权重等）。
为了预测我们使用函数 [evalMLMethod](../functions/machine-learning-functions.md#machine_learning_methods-evalmlmethod)，这需要一个状态作为参数以及特征来预测。

<a name="stochasticlinearregression-usage-fitting"></a>

**1.** 安装

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

在这里，我们还需要将数据插入到 `train_data` 桌子 参数的数量不是固定的，它只取决于参数的数量，传递到 `linearRegressionState`. 它们都必须是数值。
请注意，带有目标值的列（我们想要学习预测）被插入作为第一个参数。

**2.** 预测

在将状态保存到表中之后，我们可以多次使用它进行预测，甚至与其他状态合并并创建新的更好的模型。

``` sql
WITH (SELECT state FROM your_model) AS model SELECT
evalMLMethod(model, param1, param2) FROM test_data
```

查询将返回一列预测值。 请注意，第一个参数 `evalMLMethod` 是 `AggregateFunctionState` 对象，接下来是要素列。

`test_data` 是一个像表 `train_data` 但可能不包含目标值。

### 注 {#agg_functions-stochasticlinearregression-notes}

1.  要合并两个模型，用户可以创建这样的查询:
    `sql  SELECT state1 + state2 FROM your_models`
    哪里 `your_models` 表包含这两个模型。 此查询将返回new `AggregateFunctionState` 对象。

2.  如果没有，用户可以获取创建的模型的权重用于自己的目的，而不保存模型 `-State` 使用combinator。
    `sql  SELECT stochasticLinearRegression(0.01)(target, param1, param2) FROM train_data`
    这种查询将拟合模型并返回其权重-首先是权重，它对应于模型的参数，最后一个是偏差。 所以在上面的例子中，查询将返回一个具有3个值的列。

**另请参阅**

-   [stochasticLogisticRegression](#agg_functions-stochasticlogisticregression)
-   [线性回归和逻辑回归之间的区别](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

## stochasticLogisticRegression {#agg_functions-stochasticlogisticregression}

该函数实现随机逻辑回归。 它可以用于二进制分类问题，支持与stochasticLinearRegression相同的自定义参数，并以相同的方式工作。

### 参数 {#agg_functions-stochasticlogisticregression-parameters}

参数与stochasticLinearRegression中的参数完全相同:
`learning rate`, `l2 regularization coefficient`, `mini-batch size`, `method for updating weights`.
欲了解更多信息，请参阅 [参数](#agg_functions-stochasticlinearregression-parameters).

``` text
stochasticLogisticRegression(1.0, 1.0, 10, 'SGD')
```

**1.** 安装

<!-- -->

    参考stochasticLinearRegression相关文档

    预测标签的取值范围为[-1, 1]

**2.** 预测

<!-- -->

    使用已经保存的state我们可以预测标签为 `1` 的对象的概率。

    ``` sql
    WITH (SELECT state FROM your_model) AS model SELECT
    evalMLMethod(model, param1, param2) FROM test_data
    ```

    查询结果返回一个列的概率。注意 `evalMLMethod` 的第一个参数是 `AggregateFunctionState` 对象，接下来的参数是列的特性。

    我们也可以设置概率的范围， 这样需要给元素指定不同的标签。

    ``` sql
    SELECT ans < 1.1 AND ans > 0.5 FROM
    (WITH (SELECT state FROM your_model) AS model SELECT
    evalMLMethod(model, param1, param2) AS ans FROM test_data)
    ```

    结果是标签。

    `test_data` 是一个像 `train_data` 一样的表，但是不包含目标值。

**另请参阅**

-   [随机指标线上回归](#agg_functions-stochasticlinearregression)
-   [线性回归和逻辑回归之间的差异](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)


[原始文章](https://clickhouse.tech/docs/en/query_language/agg_functions/reference/) <!--hide-->
