---
slug: /ja/sql-reference/aggregate-functions/reference/stochasticlogisticregression
sidebar_position: 193
---

# stochasticLogisticRegression

この関数は確率的ロジスティック回帰を実装します。これは二値分類問題に使用でき、stochasticLinearRegressionと同じカスタムパラメータをサポートし、同様に動作します。

### パラメータ

パラメータはstochasticLinearRegressionと全く同じです：  
`学習率`、`L2正則化係数`、`ミニバッチサイズ`、`重みを更新する方法`。詳細については[パラメータ](../reference/stochasticlinearregression.md/#parameters)を参照してください。

``` text
stochasticLogisticRegression(1.0, 1.0, 10, 'SGD')
```

**1.** フィッティング

<!-- -->

   `Fitting`セクションの説明については、[stochasticLinearRegression](#stochasticlinearregression-usage-fitting)を参照してください。

   予測されるラベルは\[-1, 1\]でなければなりません。

**2.** 予測

<!-- -->

   保存された状態を使用して、オブジェクトがラベル`1`を持つ確率を予測できます。

   ``` sql
   WITH (SELECT state FROM your_model) AS model SELECT
   evalMLMethod(model, param1, param2) FROM test_data
   ```

   クエリは確率のカラムを返します。`evalMLMethod`の最初の引数は`AggregateFunctionState`オブジェクトであり、その次に特徴のカラムが続きます。

   また、異なるラベルを要素に割り当てる確率の境界を設定することもできます。

   ``` sql
   SELECT ans < 1.1 AND ans > 0.5 FROM
   (WITH (SELECT state FROM your_model) AS model SELECT
   evalMLMethod(model, param1, param2) AS ans FROM test_data)
   ```

   これにより、結果はラベルになります。

   `test_data`は`train_data`のようなテーブルですが、目標値を含まない場合もあります。

**関連項目**

- [stochasticLinearRegression](../../../sql-reference/aggregate-functions/reference/stochasticlinearregression.md#agg_functions-stochasticlinearregression)
- [線形回帰とロジスティック回帰の違い。](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)
