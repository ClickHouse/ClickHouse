---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: AggregatingMergeTree
---

# Aggregatingmergetree {#aggregatingmergetree}

エンジンはから継承します [メルゲツリー](mergetree.md#table_engines-mergetree)、データ部分のマージのロジックを変更する。 ClickHouseは、すべての行を同じ主キー（またはより正確には同じキー）で置き換えます [ソートキー](mergetree.md)）集計関数の状態の組み合わせを格納する単一の行（一つのデータ部分内）を持つ。

以下を使用できます `AggregatingMergeTree` 集計されたマテリアライズドビューを含む、増分データ集計用の表。

エンジンは、次の型のすべての列を処理します:

-   [AggregateFunction](../../../sql-reference/data-types/aggregatefunction.md)
-   [SimpleAggregateFunction](../../../sql-reference/data-types/simpleaggregatefunction.md)

使用することは適切です `AggregatingMergeTree` 注文によって行数を減らす場合。

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

説明リクエストパラメータの参照 [要求の説明](../../../sql-reference/statements/create.md).

**クエリ句**

を作成するとき `AggregatingMergeTree` 同じテーブル [句](mergetree.md) を作成するときのように必要です。 `MergeTree` テーブル。

<details markdown="1">

<summary>推奨されていません法テーブルを作成する</summary>

!!! attention "注意"
    可能であれば、古いプロジェクトを上記の方法に切り替えてください。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

すべてのパラメータは、inと同じ意味を持ちます `MergeTree`.
</details>

## 選択と挿入 {#select-and-insert}

データを挿入するには、 [INSERT SELECT](../../../sql-reference/statements/insert-into.md) aggregate-State-functionsを使用したクエリ。
データを選択するとき `AggregatingMergeTree` テーブル、使用 `GROUP BY` データを挿入するときと同じ集計関数ですが、 `-Merge` 接尾辞。

の結果 `SELECT` クエリ、の値 `AggregateFunction` typeは、すべてのClickHouse出力形式に対して実装固有のバイナリ表現を持ちます。 たとえば、データをダンプする場合, `TabSeparated` フォーマット `SELECT` 次に、このダンプを次のようにロードします `INSERT` クエリ。

## 集約マテリアライズドビューの例 {#example-of-an-aggregated-materialized-view}

`AggregatingMergeTree` これは、 `test.visits` テーブル:

``` sql
CREATE MATERIALIZED VIEW test.basic
ENGINE = AggregatingMergeTree() PARTITION BY toYYYYMM(StartDate) ORDER BY (CounterID, StartDate)
AS SELECT
    CounterID,
    StartDate,
    sumState(Sign)    AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY CounterID, StartDate;
```

にデータを挿入する `test.visits` テーブル。

``` sql
INSERT INTO test.visits ...
```

データはテーブルとビューの両方に挿入されます `test.basic` それは集計を実行します。

集計データを取得するには、次のようなクエリを実行する必要があります `SELECT ... GROUP BY ...` ビューから `test.basic`:

``` sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.basic
GROUP BY StartDate
ORDER BY StartDate;
```

[元の記事](https://clickhouse.com/docs/en/operations/table_engines/aggregatingmergetree/) <!--hide-->
