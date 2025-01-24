---
slug: /ja/sql-reference/aggregate-functions/reference/stochasticlinearregression
sidebar_position: 192
---

# stochasticLinearRegression {#agg_functions_stochasticlinearregression_parameters}

この関数は確率的線形回帰を実装します。学習率、L2正則化係数、ミニバッチサイズのカスタムパラメータをサポートし、重み更新のためにいくつかの方法を持っています（デフォルトで使用される[Adam](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Adam)、[simple SGD](https://en.wikipedia.org/wiki/Stochastic_gradient_descent)、[Momentum](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Momentum)、および[Nesterov](https://mipt.ru/upload/medialibrary/d7e/41-91.pdf)）。

### パラメータ

4つのカスタマイズ可能なパラメータがあります。これらは順番に関数に渡されますが、すべてを渡す必要はありません - デフォルト値が使用されますが、良いモデルにはパラメータの調整が必要です。

``` text
stochasticLinearRegression(0.00001, 0.1, 15, 'Adam')
```

1. `学習率` は勾配降下法のステップが行われる際のステップ長の係数です。学習率が大きすぎるとモデルの重みが無限大になる可能性があります。デフォルトは `0.00001` です。
2. `L2正則化係数` で、過学習を防ぐのに役立つかもしれません。デフォルトは `0.1` です。
3. `ミニバッチサイズ` は勾配が計算されて1ステップの勾配降下を実行する要素の数を設定します。純粋な確率的降下は1つの要素を使用しますが、小さなバッチ（約10要素）を使用することで勾配ステップがより安定します。デフォルトは `15` です。
4. `重み更新のための方法` は、`Adam`（デフォルト）、`SGD`、`Momentum`、`Nesterov` です。`Momentum`と`Nesterov`は計算とメモリが少し多く必要ですが、収束の速さと確率的勾配法の安定性において有用であることがあります。

### 使用法

`stochasticLinearRegression`は、モデルのフィッティングと新しいデータへの予測の2つのステップで使用されます。モデルをフィットしてその状態を後で使用するために保存するには、`-State` コンビネータを使用します。これは状態（例：モデルの重み）を保存します。
予測するためには、[evalMLMethod](../../../sql-reference/functions/machine-learning-functions.md#machine_learning_methods-evalmlmethod)関数を使用します。この関数は、予測する特徴とともに状態を引数として受け取ります。

<a name="stochasticlinearregression-usage-fitting"></a>

**1.** フィッティング

以下のようなクエリを使用することができます。

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

ここでは、`train_data` テーブルにデータを挿入する必要があります。パラメータの数は固定されておらず、`linearRegressionState`に渡される引数の数にのみ依存します。すべて数値でなければなりません。
予測したい対象値を持つカラムは、最初の引数として挿入されます。

**2.** 予測

テーブルに状態を保存した後、それを複数回使用して予測を行うことができますし、他の状態と結合して新しい、さらに優れたモデルを作成することもできます。

``` sql
WITH (SELECT state FROM your_model) AS model SELECT
evalMLMethod(model, param1, param2) FROM test_data
```

このクエリは予測値のカラムを返します。`evalMLMethod` の最初の引数は `AggregateFunctionState` オブジェクトであり、その後が特徴のカラムです。

`test_data`は`train_data`と同様のテーブルですが、対象値が含まれていない可能性があります。

### 注意

1.  2つのモデルをマージするためにユーザーはこのようなクエリを作成できます：
    `sql  SELECT state1 + state2 FROM your_models`
    ここで `your_models` テーブルは両方のモデルを含みます。このクエリは新しい `AggregateFunctionState` オブジェクトを返します。

2.  ユーザーは、`-State` コンビネータが使用されていない場合作成したモデルの重みを保存せずに取得することができます。
    `sql  SELECT stochasticLinearRegression(0.01)(target, param1, param2) FROM train_data`
    このようなクエリはモデルをフィットし、その重みを返します - 最初はモデルのパラメータに対応する重みで、最後がバイアスです。上記の例では、クエリは3つの値を持つカラムを返します。

**参照**

- [stochasticLogisticRegression](../../../sql-reference/aggregate-functions/reference/stochasticlogisticregression.md#stochasticlogisticregression)
- [線形回帰とロジスティック回帰の違い](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)
