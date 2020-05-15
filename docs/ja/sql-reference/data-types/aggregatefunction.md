---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 52
toc_title: AggregateFunction(name,types_of_arguments)...)
---

# AggregateFunction(name, types\_of\_arguments…) {#data-type-aggregatefunction}

Aggregate functions can have an implementation-defined intermediate state that can be serialized to an AggregateFunction(…) data type and stored in a table, usually, by means of [マテリアライズドビュー](../../sql-reference/statements/select.md#create-view). 集計関数の状態を生成する一般的な方法は、集計関数を呼び出すことです `-State` 接尾辞。 将来の集約の最終結果を得るには、同じ集計関数を使用する必要があります `-Merge`接尾辞。

`AggregateFunction` — parametric data type.

**パラメータ**

-   集計関数の名前。

        If the function is parametric, specify its parameters too.

-   集計関数の引数の型です。

**例えば**

``` sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

[uniq](../../sql-reference/aggregate-functions/reference.md#agg_function-uniq),アーニフ ([任意の](../../sql-reference/aggregate-functions/reference.md#agg_function-any)+[もし](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-if))と [分位数](../../sql-reference/aggregate-functions/reference.md) ClickHouseでサポートされている集計関数です。

## 使い方 {#usage}

### データ挿入 {#data-insertion}

データを挿入するには `INSERT SELECT` 総計を使って `-State`-機能。

**関数の例**

``` sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

対応する機能とは対照的に `uniq` と `quantiles`, `-State`-関数は、最終的な値の代わりに状態を返します。 言い換えれば、それらは次の値を返します `AggregateFunction` タイプ。

の結果で `SELECT` クエリ、値の `AggregateFunction` タイプは、すべてのClickHouse出力形式に対して実装固有のバイナリ表現を持ちます。 たとえば、データをダンプする場合, `TabSeparated` フォーマット `SELECT` このダンプは、次のようにロードされます `INSERT` クエリ。

### データ選択 {#data-selection}

データを選択するとき `AggregatingMergeTree` テーブル、使用 `GROUP BY` 句とデータを挿入するときと同じ集約関数が、 `-Merge`接尾辞。

以下の集計関数 `-Merge` suffixは、状態のセットを取得し、それらを結合し、完全なデータ集約の結果を返します。

たとえば、次の二つのクエリは同じ結果を返します:

``` sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

## 使用例 {#usage-example}

見る [ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂづｭﾂつｹ](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) エンジンの説明。

[元の記事](https://clickhouse.tech/docs/en/data_types/nested_data_structures/aggregatefunction/) <!--hide-->
