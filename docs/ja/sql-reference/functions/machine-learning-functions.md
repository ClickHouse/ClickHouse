---
slug: /ja/sql-reference/functions/machine-learning-functions
sidebar_position: 115
sidebar_label: 機械学習
---

# 機械学習関数

## evalMLMethod

フィットした回帰モデルを使用した予測には、`evalMLMethod` 関数を使用します。`linearRegression` のリンクを参照してください。

## stochasticLinearRegression

[stochasticLinearRegression](../../sql-reference/aggregate-functions/reference/stochasticlinearregression.md#agg_functions-stochasticlinearregression) 集約関数は、線形モデルと平均二乗誤差（MSE）損失関数を使用した確率的勾配降下法を実装します。新しいデータに対して予測を行う際には `evalMLMethod` を使用します。

## stochasticLogisticRegression

[stochasticLogisticRegression](../../sql-reference/aggregate-functions/reference/stochasticlogisticregression.md#agg_functions-stochasticlogisticregression) 集約関数は、バイナリ分類問題のための確率的勾配降下法を実装します。新しいデータに対して予測を行う際には `evalMLMethod` を使用します。
