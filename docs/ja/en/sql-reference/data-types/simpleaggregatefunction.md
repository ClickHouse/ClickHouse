---
description: 'SimpleAggregateFunction データ型のドキュメント'
sidebar_label: 'SimpleAggregateFunction'
sidebar_position: 48
slug: /sql-reference/data-types/simpleaggregatefunction
title: 'SimpleAggregateFunction 型'
doc_type: 'reference'
---

<div id="description">
  ## 説明
</div>

`SimpleAggregateFunction` データ型は、集約関数の中間状態を格納しますが、[`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md)
型のように完全な状態を格納するわけではありません。

この最適化は、次の性質を満たす関数に適用できます。

> 行集合 `S1 UNION ALL S2` に関数 `f` を適用した結果は、行集合の各部分に個別に `f` を適用し、その結果に対して再度
> `f` を適用することで得られます: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`。

この性質により、結合後の結果を計算するには部分的な集約結果だけで十分であることが保証されるため、
追加のデータを保存したり処理したりする必要はありません。たとえば、
`min` や `max` 関数では、中間結果から最終結果を計算するための追加手順は不要ですが、`avg` 関数では
合計値と件数を保持しておく必要があり、これらは中間状態を結合する最後の `Merge` ステップで除算されて
平均値が求められます。

集約関数の値は通常、関数名の末尾に [`-SimpleState`](/ja/sql-reference/aggregate-functions/combinators#-simplestate) コンビネータを付けて
集約関数を呼び出すことで生成されます。

<div id="syntax">
  ## 構文
</div>

```sql
SimpleAggregateFunction(aggregate_function_name, types_of_arguments...)
```

**パラメータ**

* `aggregate_function_name` - 集約関数の名前。
* `Type` - 集約関数の引数の型。

<div id="supported-functions">
  ## サポートされている関数
</div>

以下の集約関数がサポートされています。

* [`any`](/ja/sql-reference/aggregate-functions/reference/any.md)
* [`any_respect_nulls`](/ja/sql-reference/aggregate-functions/reference/any.md)
* [`anyLast`](/ja/sql-reference/aggregate-functions/reference/anyLast.md)
* [`anyLast_respect_nulls`](/ja/sql-reference/aggregate-functions/reference/anyLast.md)
* [`min`](/ja/sql-reference/aggregate-functions/reference/min.md)
* [`max`](/ja/sql-reference/aggregate-functions/reference/max.md)
* [`sum`](/ja/sql-reference/aggregate-functions/reference/sum.md)
* [`sumWithOverflow`](/ja/sql-reference/aggregate-functions/reference/sumWithOverflow.md)
* [`groupBitAnd`](/ja/sql-reference/aggregate-functions/reference/groupBitAnd.md)
* [`groupBitOr`](/ja/sql-reference/aggregate-functions/reference/groupBitOr.md)
* [`groupBitXor`](/ja/sql-reference/aggregate-functions/reference/groupBitXor.md)
* [`groupArrayArray`](/ja/sql-reference/aggregate-functions/reference/groupArrayArray.md)
* [`groupUniqArrayArray`](../../sql-reference/aggregate-functions/reference/groupUniqArray.md)
* [`groupUniqArrayArrayMap`](../../sql-reference/aggregate-functions/combinators#-map)
* [`sumMap` (`sumMappedArrays`)](/ja/sql-reference/aggregate-functions/reference/sumMappedArrays.md)
* [`minMap` (`minMappedArrays`)](/ja/sql-reference/aggregate-functions/reference/minMappedArrays.md)
* [`maxMap` (`maxMappedArrays`)](/ja/sql-reference/aggregate-functions/reference/maxMappedArrays.md)

:::note
`SimpleAggregateFunction(func, Type)` の値は同じ `Type` になるため、
`AggregateFunction` 型とは異なり、`-Merge`/`-State` 集約関数コンビネータを
適用する必要はありません。

同じ集約関数であれば、`SimpleAggregateFunction` 型は
`AggregateFunction` よりも高いパフォーマンスを発揮します。
:::

<div id="example">
  ## 例
</div>

```sql
CREATE TABLE simple (id UInt64, val SimpleAggregateFunction(sum, Double)) ENGINE=AggregatingMergeTree ORDER BY id;
```

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouse で集約関数コンビネータを使用する](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)    - ブログ: [ClickHouse で集約関数コンビネータを使用する](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
* [AggregateFunction](/ja/sql-reference/data-types/aggregatefunction) 型。