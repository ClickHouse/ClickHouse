---
machine_translated: true
machine_translated_rev: 71d72c1f237f4a553fe91ba6c6c633e81a49e35b
---

# SimpleAggregateFunction {#data-type-simpleaggregatefunction}

`SimpleAggregateFunction(name, types_of_arguments…)` データ型は、集計関数の現在の値を格納し、その完全な状態を次のように格納しません [`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md) そうだ この最適化は、次のプロパティが保持される関数に適用できます。 `f` 行セットに `S1 UNION ALL S2` 取得できるよ `f` 行の一部に別々に設定し、再び適用します `f` 結果に: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`. このプロパティは、部分集計の結果が結合された結果を計算するのに十分であることを保証するため、余分なデータを格納して処理する必要はあり

次の集計関数がサポートされます:

-   [`any`](../../sql-reference/aggregate-functions/reference.md#agg_function-any)
-   [`anyLast`](../../sql-reference/aggregate-functions/reference.md#anylastx)
-   [`min`](../../sql-reference/aggregate-functions/reference.md#agg_function-min)
-   [`max`](../../sql-reference/aggregate-functions/reference.md#agg_function-max)
-   [`sum`](../../sql-reference/aggregate-functions/reference.md#agg_function-sum)
-   [`groupBitAnd`](../../sql-reference/aggregate-functions/reference.md#groupbitand)
-   [`groupBitOr`](../../sql-reference/aggregate-functions/reference.md#groupbitor)
-   [`groupBitXor`](../../sql-reference/aggregate-functions/reference.md#groupbitxor)
-   [`groupArrayArray`](../../sql-reference/aggregate-functions/reference.md#agg_function-grouparray)
-   [`groupUniqArrayArray`](../../sql-reference/aggregate-functions/reference.md#groupuniqarrayx-groupuniqarraymax-sizex)

の値 `SimpleAggregateFunction(func, Type)` 見て、同じように格納 `Type` したがって、次の関数を適用する必要はありません `-Merge`/`-State` 接尾辞。 `SimpleAggregateFunction` は以上のパフォーマンス `AggregateFunction` 同じ集計機能を使って。

**パラメータ**

-   集計関数の名前。
-   集計関数の引数の型。

**例**

``` sql
CREATE TABLE t
(
    column1 SimpleAggregateFunction(sum, UInt64),
    column2 SimpleAggregateFunction(any, String)
) ENGINE = ...
```

[元の記事](https://clickhouse.com/docs/en/data_types/simpleaggregatefunction/) <!--hide-->
