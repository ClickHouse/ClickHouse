---
slug: /ja/sql-reference/data-types/aggregatefunction
sidebar_position: 46
sidebar_label: AggregateFunction
---

# AggregateFunction

集約関数は、`AggregateFunction(...)` データ型にシリアライズできる実装定義の中間状態を持つことができ、通常は[マテリアライズドビュー](../../sql-reference/statements/create/view.md)によってテーブルに保存されます。集約関数の状態を生成する一般的な方法は、`-State` サフィックスを付けて集約関数を呼び出すことです。将来、集約の最終結果を得るには、同じ集約関数を `-Merge` サフィックスとともに使用する必要があります。

`AggregateFunction(name, types_of_arguments...)` — パラメトリックデータ型。

**パラメータ**

- 集約関数の名前。関数がパラメトリックな場合、そのパラメータも指定します。

- 集約関数引数の型。

**例**

``` sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

[uniq](../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)、anyIf（[any](../../sql-reference/aggregate-functions/reference/any.md#agg_function-any)+[If](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-if)）および [quantiles](../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) は、ClickHouseでサポートされている集約関数です。

## 使用方法

### データの挿入

データを挿入するには、集約の `-State`- 関数を使用して `INSERT SELECT` を使用します。

**関数の例**

``` sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

対応する `uniq` と `quantiles` 関数と対照的に、`-State`- 関数は最終値ではなく状態を返します。つまり、`AggregateFunction` 型の値を返します。

`SELECT` クエリの結果では、`AggregateFunction` 型の値は、ClickHouse のすべての出力フォーマットに対して実装固有のバイナリ表現を持っています。例えば、`SELECT` クエリで `TabSeparated` フォーマットにダンプを出力した場合、このダンプは `INSERT` クエリを使用して読み込むことができます。

### データの選択

`AggregatingMergeTree` テーブルからデータを選択する際は、`GROUP BY` 句と、データを挿入するときと同じ集約関数を使用しますが、`-Merge` サフィックスを使用します。

`-Merge` サフィックスを持つ集約関数は、一連の状態を取り、それらを結合して完全なデータ集約の結果を返します。

例えば、次の2つのクエリは同じ結果を返します：

``` sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

## 使用例

[AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) エンジンの説明を参照。

## 関連コンテンツ

- ブログ: [ClickHouseでの集約コンビネーターの活用](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
