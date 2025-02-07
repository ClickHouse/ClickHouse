---
slug: /ja/sql-reference/data-types/simpleaggregatefunction
sidebar_position: 48
sidebar_label: SimpleAggregateFunction
---

# SimpleAggregateFunction

`SimpleAggregateFunction(name, types_of_arguments...)` データ型は、集約関数の現在の値を格納し、[`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md) のようにその完全な状態を格納しません。この最適化は、次の性質を持つ関数に適用できます。すなわち、行セット `S1 UNION ALL S2` に関数 `f` を適用した結果は、行セットの部分に対してそれぞれ `f` を適用し、その結果に再度 `f` を適用することで得られる: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))` 。この性質により、部分的な集計結果で結合結果を計算するのに十分であることが保証されるため、余分なデータを格納および処理する必要がなくなります。

集約関数の値を生成する一般的な方法は、[-SimpleState](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-simplestate) サフィックスを付けて集約関数を呼び出すことです。

サポートされている集約関数は以下の通りです:

- [`any`](../../sql-reference/aggregate-functions/reference/any.md#agg_function-any)
- [`anyLast`](../../sql-reference/aggregate-functions/reference/anylast.md#anylastx)
- [`min`](../../sql-reference/aggregate-functions/reference/min.md#agg_function-min)
- [`max`](../../sql-reference/aggregate-functions/reference/max.md#agg_function-max)
- [`sum`](../../sql-reference/aggregate-functions/reference/sum.md#agg_function-sum)
- [`sumWithOverflow`](../../sql-reference/aggregate-functions/reference/sumwithoverflow.md#sumwithoverflowx)
- [`groupBitAnd`](../../sql-reference/aggregate-functions/reference/groupbitand.md#groupbitand)
- [`groupBitOr`](../../sql-reference/aggregate-functions/reference/groupbitor.md#groupbitor)
- [`groupBitXor`](../../sql-reference/aggregate-functions/reference/groupbitxor.md#groupbitxor)
- [`groupArrayArray`](../../sql-reference/aggregate-functions/reference/grouparray.md#agg_function-grouparray)
- [`groupUniqArrayArray`](../../sql-reference/aggregate-functions/reference/groupuniqarray.md)
- [`groupUniqArrayArrayMap`](../../sql-reference/aggregate-functions/combinators#-map)
- [`sumMap`](../../sql-reference/aggregate-functions/reference/summap.md#agg_functions-summap)
- [`minMap`](../../sql-reference/aggregate-functions/reference/minmap.md#agg_functions-minmap)
- [`maxMap`](../../sql-reference/aggregate-functions/reference/maxmap.md#agg_functions-maxmap)

:::note
`SimpleAggregateFunction(func, Type)` の値は `Type` と同じように見え、格納されますので、`-Merge`/`-State` サフィックスを付けた関数を適用する必要はありません。

`SimpleAggregateFunction` は同じ集約関数を持つ `AggregateFunction` よりも性能が良くなります。
:::

**パラメーター**

- 集約関数の名前。
- 集約関数の引数の型。

**例**

``` sql
CREATE TABLE simple (id UInt64, val SimpleAggregateFunction(sum, Double)) ENGINE=AggregatingMergeTree ORDER BY id;
```

