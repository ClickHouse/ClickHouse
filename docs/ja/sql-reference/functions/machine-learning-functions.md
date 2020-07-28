---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 64
toc_title: "\u6A5F\u68B0\u5B66\u7FD2\u6A5F\u80FD"
---

# 機械学習機能 {#machine-learning-functions}

## evalMLMethod(予測) {#machine_learning_methods-evalmlmethod}

近似回帰モデルを使用した予測では `evalMLMethod` 機能。 リンクを参照 `linearRegression`.

### 確率的線形回帰 {#stochastic-linear-regression}

その [stochasticLinearRegression](../../sql-reference/aggregate-functions/reference.md#agg_functions-stochasticlinearregression) 集計関数は線形モデルとMSE損失関数を用いた確率勾配降下法を実装する。 用途 `evalMLMethod` 新しいデータを予測する。

### 確率ロジスティック回帰 {#stochastic-logistic-regression}

その [stochasticLogisticRegression](../../sql-reference/aggregate-functions/reference.md#agg_functions-stochasticlogisticregression) 集計関数は、バイナリ分類問題のための確率勾配降下法を実装しています。 用途 `evalMLMethod` 新しいデータを予測する。
