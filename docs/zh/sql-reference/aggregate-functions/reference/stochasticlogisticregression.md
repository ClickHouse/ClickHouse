---
sidebar_position: 222
---

# stochasticLogisticRegression {#agg_functions-stochasticlogisticregression}

该函数实现随机逻辑回归。 它可以用于二进制分类问题，支持与stochasticLinearRegression相同的自定义参数，并以相同的方式工作。

### 参数 {#agg_functions-stochasticlogisticregression-parameters}

参数与stochasticLinearRegression中的参数完全相同:
`learning rate`, `l2 regularization coefficient`, `mini-batch size`, `method for updating weights`.
欲了解更多信息，参见 [参数] (#agg_functions-stochasticlinearregression-parameters).

**语法**

``` sql
stochasticLogisticRegression(1.0, 1.0, 10, 'SGD')
```

**1.** 拟合

<!-- -->

    参考[stochasticLinearRegression](#stochasticlinearregression-usage-fitting)  `拟合` 章节文档。

    预测标签的取值范围为\[-1, 1\]

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

**参见**

-   [随机指标线性回归](../../../sql-reference/aggregate-functions/reference/stochasticlinearregression.md#agg_functions-stochasticlinearregression)
-   [线性回归和逻辑回归之间的差异](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)
