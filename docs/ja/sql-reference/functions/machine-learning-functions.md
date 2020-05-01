---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 64
toc_title: "\u6A5F\u68B0\u5B66\u7FD2\u306E\u6A5F\u80FD"
---

# 機械学習の機能 {#machine-learning-functions}

## evalMLMethod(予測) {#machine_learning_methods-evalmlmethod}

適合回帰モデルを使用した予測 `evalMLMethod` 機能。 リンクを見る `linearRegression`.

### 確率的線形回帰 {#stochastic-linear-regression}

その [stochasticLinearRegression](../../sql-reference/aggregate-functions/reference.md#agg_functions-stochasticlinearregression) 集合関数は，線形モデルとＭＳＥ損失関数を用いた確率的勾配降下法を実装する。 使用 `evalMLMethod` 新しいデータを予測する。

### 確率論的ロジスティック回帰 {#stochastic-logistic-regression}

その [stochasticLogisticRegression](../../sql-reference/aggregate-functions/reference.md#agg_functions-stochasticlogisticregression) 集合関数は，二値分類問題に対して確率的勾配降下法を実装する。 使用 `evalMLMethod` 新しいデータを予測する。
