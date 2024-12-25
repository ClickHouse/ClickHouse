---
slug: /ja/engines/table-engines/mergetree-family/aggregatingmergetree
sidebar_position: 60
sidebar_label: AggregatingMergeTree
---

# AggregatingMergeTree

このエンジンは、データパーツのマージロジックを変更した[MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engines-mergetree)から継承しています。ClickHouseでは、同じ主キー（より正確には、同じ[ソートキー](../../../engines/table-engines/mergetree-family/mergetree.md)）を持つすべての行を、そのキーを持つすべての行の結合状態を保存する単一の行に置き換えます（同一のデータパーツ内で）。

`AggregatingMergeTree`テーブルは、集計されたマテリアライズドビューを含む増分データ集計に使用できます。

このエンジンは、以下の型のすべてのカラムを処理します：

## [AggregateFunction](../../../sql-reference/data-types/aggregatefunction.md)
## [SimpleAggregateFunction](../../../sql-reference/data-types/simpleaggregatefunction.md)

`AggregatingMergeTree`を使用するのが適切なのは、行数を何桁も削減できる場合です。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = AggregatingMergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[TTL expr]
[SETTINGS name=value, ...]
```

リクエストパラメータの説明については、[リクエストの説明](../../../sql-reference/statements/create/table.md)を参照してください。

**クエリ句**

`AggregatingMergeTree`テーブルを作成するときには、`MergeTree`テーブルを作成するときと同じ[句](../../../engines/table-engines/mergetree-family/mergetree.md)が必要です。

<details markdown="1">

<summary>非推奨のテーブル作成メソッド</summary>

:::note
新しいプロジェクトではこの方法を使用せず、可能であれば古いプロジェクトを上記の方法に切り替えてください。
:::

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

すべてのパラメータは、`MergeTree`と同じ意味を持ちます。
</details>

## SELECTとINSERT {#select-and-insert}

データを挿入するには、集計-State-関数を使用して[INSERT SELECT](../../../sql-reference/statements/insert-into.md)クエリを使用します。
`AggregatingMergeTree`テーブルからデータを選択する際には、`GROUP BY`句と挿入時に使用したものと同じ集計関数を、`-Merge`サフィックスをつけて使用します。

`SELECT`クエリの結果において、`AggregateFunction`タイプの値は、すべてのClickHouse出力フォーマットに対して実装依存のバイナリ表現を持ちます。たとえば、`TabSeparated`フォーマットで`SELECT`クエリによってデータをダンプする場合、このダンプは`INSERT`クエリを使用して再ロードできます。

## 集計マテリアライズドビューの例 {#example-of-an-aggregated-materialized-view}

以下の例では、`test`という名前のデータベースがすでにあると仮定していますので、まだ存在しない場合は作成してください：

```sql
CREATE DATABASE test;
```

次に、生データを含むテーブル`test.visits`を作成します：

``` sql
CREATE TABLE test.visits
 (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Sign Nullable(Int32),
    UserID Nullable(Int32)
) ENGINE = MergeTree ORDER BY (StartDate, CounterID);
```

次に、総訪問数と一意のユーザー数を追跡する`AggregationFunction`を格納する`AggregatingMergeTree`テーブルが必要です。

`test.visits`テーブルを監視し、`AggregateFunction`タイプを使用する`AggregatingMergeTree`マテリアライズドビューを作成します：

``` sql
CREATE TABLE test.agg_visits (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Visits AggregateFunction(sum, Nullable(Int32)),
    Users AggregateFunction(uniq, Nullable(Int32))
)
ENGINE = AggregatingMergeTree() ORDER BY (StartDate, CounterID);
```

`test.visits`から`test.agg_visits`にデータを入力するマテリアライズドビューを作成します：

```sql
CREATE MATERIALIZED VIEW test.visits_mv TO test.agg_visits
AS SELECT
    StartDate,
    CounterID,
    sumState(Sign) AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY StartDate, CounterID;
```

`test.visits`テーブルにデータを挿入します：

``` sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
 VALUES (1667446031000, 1, 3, 4), (1667446031000, 1, 6, 3);
```

データは`test.visits`と`test.agg_visits`の両方に挿入されます。

集計されたデータを取得するには、`SELECT ... GROUP BY ...`クエリをマテリアライズドビュー`test.mv_visits`から実行します：

```sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.agg_visits
GROUP BY StartDate
ORDER BY StartDate;
```

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │      9 │     2 │
└─────────────────────────┴────────┴───────┘
```

`test.visits`にさらに2つのレコードを追加しますが、今回は1つのレコードに異なるタイムスタンプを使用してみます：

```sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
 VALUES (1669446031000, 2, 5, 10), (1667446031000, 3, 7, 5);
```

再び`SELECT`クエリを実行すると、以下の出力が返されます：

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │     16 │     3 │
│ 2022-11-26 07:00:31.000 │      5 │     1 │
└─────────────────────────┴────────┴───────┘
```

## 関連コンテンツ

- ブログ: [Using Aggregate Combinators in ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
