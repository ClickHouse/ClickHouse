---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 35
toc_title: "\uFF82\u3064\uFF68\uFF82\u59EA\"\uFF82\u3064\"\uFF82\u50B5\uFF82\u3065\
  \uFF6D\uFF82\u3064\uFF79"
---

# ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂづｭﾂつｹ {#aggregatingmergetree}

エンジンは [MergeTree](mergetree.md#table_engines-mergetree)、データパーツのマージのロジックを変更する。 ClickHouseは、すべての行を同じ主キー（またはより正確には同じ）で置き換えます [ソートキー](mergetree.md)）集計関数の状態の組み合わせを格納する単一の行（一つのデータ部門内）。

を使用することができ `AggregatingMergeTree` テーブルが増えた場合のデータ収集、集計を実現します。

エンジンプロセスの全てのカラム [AggregateFunction](../../../sql-reference/data-types/aggregatefunction.md) タイプ。

使用するのが適切です `AggregatingMergeTree` 注文によって行数が減る場合。

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

作成するとき `AggregatingMergeTree` テーブル同じ [句](mergetree.md) 作成するときと同じように、必須です。 `MergeTree` テーブル。

<details markdown="1">

<summary>テーブルを作成する非推奨の方法</summary>

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

すべてのパラメーターの意味は、次のようになります `MergeTree`.
</details>

## 選択して挿入 {#select-and-insert}

データを挿入するには [INSERT SELECT](../../../sql-reference/statements/insert-into.md) aggregate-State-functionsを使用したクエリ。
データを選択するとき `AggregatingMergeTree` テーブル、使用 `GROUP BY` 句とデータを挿入するときと同じ集約関数が、 `-Merge` 接尾辞。

の結果で `SELECT` クエリ、値の `AggregateFunction` タイプは、すべてのClickHouse出力形式に対して実装固有のバイナリ表現を持ちます。 たとえば、データをダンプする場合, `TabSeparated` フォーマット `SELECT` このダンプは、次のようにロードされます `INSERT` クエリ。

## 集約マテリアライズドビューの例 {#example-of-an-aggregated-materialized-view}

`AggregatingMergeTree` マテリアライズドビュー `test.visits` テーブル:

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

データを挿入する `test.visits` テーブル。

``` sql
INSERT INTO test.visits ...
```

データは、テーブルとビューの両方に挿入されます `test.basic` それは集約を実行します。

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

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/aggregatingmergetree/) <!--hide-->
