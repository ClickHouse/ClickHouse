---
description: '集約関数の中間状態を保存する ClickHouse の AggregateFunction データ型に関するドキュメント'
keywords: ['AggregateFunction', '型']
sidebar_label: 'AggregateFunction'
sidebar_position: 46
slug: /sql-reference/data-types/aggregatefunction
title: 'AggregateFunction 型'
doc_type: 'reference'
---

<div id="description">
  ## 説明
</div>

ClickHouse のすべての[集約関数](/ja/sql-reference/aggregate-functions)には、
実装固有の中間状態があり、これをシリアライズして
`AggregateFunction` データ型として table に保存できます。これは通常、
[materialized view](../../sql-reference/statements/create/view.md)を
使用して行われます。

`AggregateFunction` 型で一般的に使用される集約関数[コンビネータ](/ja/sql-reference/aggregate-functions/combinators)
は 2 つあります。

* [`-State`](/ja/sql-reference/aggregate-functions/combinators#-state) 集約関数コンビネータ。集約
  関数名の末尾に付加すると、`AggregateFunction` の中間状態を生成します。
* [`-Merge`](/ja/sql-reference/aggregate-functions/combinators#-merge) 集約
  関数コンビネータ。中間状態から集約の最終結果を取得するために
  使用されます。

<div id="syntax">
  ## 構文
</div>

```sql
AggregateFunction(aggregate_function_name, types_of_arguments...)
```

**パラメータ**

* `aggregate_function_name` - 集約関数の名前。関数が
  パラメータ付きの場合は、そのパラメータも指定する必要があります。
* `types_of_arguments` - 集約関数の引数の型。

例：

```sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

<div id="usage">
  ## 利用状況
</div>

<div id="data-insertion">
  ### データの挿入
</div>

`AggregateFunction` 型のカラムを持つテーブルにデータを挿入するには、
集約関数と
[`-State`](/ja/sql-reference/aggregate-functions/combinators#-state) 集約関数コンビネータを使用した
`INSERT SELECT` を利用できます。

たとえば、`AggregateFunction(uniq, UInt64)` および
`AggregateFunction(quantiles(0.5, 0.9), UInt64)` 型のカラムに挿入する場合は、以下の
コンビネータ付き集約関数を使用します。

```sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

関数 `uniq` および `quantiles` とは異なり、`uniqState` と `quantilesState`
(`-State` 集約関数コンビネータが付加されたもの) は、最終的な値ではなく状態を返します。
つまり、これらは `AggregateFunction` 型の値を返します。

`SELECT` クエリの結果では、`AggregateFunction` 型の値は、すべての ClickHouse 出力
フォーマットにおいて、実装固有のバイナリ表現を持ちます。

入力値から状態を構築できる特別な Session レベルの設定 `aggregate_function_input_format` があります。
サポートされるフォーマットは次のとおりです。

* `state` - シリアライズされた状態を含む binary string (デフォルト) 。
  たとえば、`SELECT`
  クエリでデータを `TabSeparated` フォーマットにダンプした場合、そのダンプは `INSERT` クエリを使って再度読み込めます。
* `value` - このフォーマットでは、aggregate function の argument の単一の値、または複数の arguments の場合はそれらの tuple を想定します。これがデシリアライズされて、対応する状態が構築されます
* `array` - このフォーマットでは、上記の value オプションで説明したとおり、値の Array を想定します。Array のすべての要素が集計されて状態が構築されます

<div id="data-selection">
  ### データの選択
</div>

`AggregatingMergeTree` テーブルからデータを選択する際は、`GROUP BY` 句と、
データを挿入したときと同じ集約関数を使用しますが、
[`-Merge`](/ja/sql-reference/aggregate-functions/combinators#-merge) コンビネータを使用します。

`-Merge` コンビネータが付加された集約関数は、一連の
状態を受け取り、それらを結合して完全なデータ集約の結果を返します。

たとえば、次の 2 つのクエリは同じ結果を返します。

```sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

<div id="usage-example">
  ## 使用例
</div>

[AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) エンジンの説明をご覧ください。

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouseでArray、Map、Stateに集約関数コンビネータを使用する](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
* [MergeState](/ja/sql-reference/aggregate-functions/combinators#-mergestate)
  コンビネータ。
* [State](/ja/sql-reference/aggregate-functions/combinators#-state) コンビネータ。