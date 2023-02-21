# 机器学习函数 {#ji-qi-xue-xi-han-shu}

## evalMLMethod（预测) {#machine_learning_methods-evalmlmethod}

使用拟合回归模型的预测请使用`evalMLMethod`函数。 请参阅`linearRegression`中的链接。

## 随机线性回归 {#stochastic-linear-regression}

`stochasticLinearRegression`聚合函数使用线性模型和MSE损失函数实现随机梯度下降法。 使用`evalMLMethod`来预测新数据。
请参阅示例和注释[此处](../../sql-reference/functions/machine-learning-functions.md#agg_functions-stochasticlinearregression)。

## 随机逻辑回归 {#stochastic-logistic-regression}

`stochasticLogisticRegression`聚合函数实现了二元分类问题的随机梯度下降法。 使用`evalMLMethod`来预测新数据。
请参阅示例和注释[此处](../../sql-reference/functions/machine-learning-functions.md#agg_functions-stochasticlogisticregression)。
