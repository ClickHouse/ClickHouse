---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 52
toc_title: "AggregateFunction(\u540D\u524D,types_of_arguments...)"
---

# AggregateFunction(name, types_of_arguments…) {#data-type-aggregatefunction}

Aggregate functions can have an implementation-defined intermediate state that can be serialized to an AggregateFunction(…) data type and stored in a table, usually, by means of [マテリアライズドビュー](../../sql-reference/statements/create.md#create-view). 集計関数の状態を生成する一般的な方法は、集計関数を呼び出すことです。 `-State` 接尾辞。 将来集計の最終結果を取得するには、同じ集計関数を使用する必要があります。 `-Merge`接尾辞。

`AggregateFunction` — parametric data type.

**パラメータ**

-   集計関数の名前。

        If the function is parametric, specify its parameters too.

-   集計関数の引数の型。

**例**

``` sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

[uniq](../../sql-reference/aggregate-functions/reference.md#agg_function-uniq),anyIf ([任意](../../sql-reference/aggregate-functions/reference.md#agg_function-any)+[もし](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-if))と [分位数](../../sql-reference/aggregate-functions/reference.md) ClickHouseでサポートされている集計関数です。

## 使用法 {#usage}

### データ挿入 {#data-insertion}

データを挿入するには、 `INSERT SELECT` 総計を使って `-State`-機能。

**関数の例**

``` sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

対応する機能とは対照的に `uniq` と `quantiles`, `-State`-関数は、最終的な値の代わりに状態を返します。 言い換えれば、彼らはの値を返します `AggregateFunction` タイプ。

の結果 `SELECT` クエリ、の値 `AggregateFunction` typeは、すべてのClickHouse出力形式に対して実装固有のバイナリ表現を持ちます。 たとえば、データをダンプする場合, `TabSeparated` フォーマット `SELECT` このダンプは、以下を使用してロードバックできます `INSERT` クエリ。

### データ選択 {#data-selection}

データを選択するとき `AggregatingMergeTree` テーブル、使用 `GROUP BY` データを挿入するときと同じ集計関数ですが、 `-Merge`接尾辞。

を持つ集合関数 `-Merge` suffixは、状態のセットを取得し、それらを結合し、完全なデータ集計の結果を返します。

たとえば、次の二つのクエリは同じ結果を返します:

``` sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

## 使用例 {#usage-example}

見る [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) エンジンの説明。

[元の記事](https://clickhouse.tech/docs/en/data_types/nested_data_structures/aggregatefunction/) <!--hide-->
