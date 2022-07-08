---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: "\u30B5\u30DF\u30F3\u30B0\u30DE\u30FC\u30B2\u30C4\u30EA\u30FC"
---

# サミングマーゲツリー {#summingmergetree}

エンジンはから継承します [メルゲツリー](mergetree.md#table_engines-mergetree). 違いは、データ部分をマージするときに `SummingMergeTree` テーブルClickHouseは、すべての行を同じ主キー（またはより正確には同じキー）で置き換えます [ソートキー](mergetree.md)）数値データ型の列の集計値を含む行。 並べ替えキーが単一のキー値が多数の行に対応するように構成されている場合、ストレージ容量が大幅に削減され、データ選択が高速化されます。

エンジンを一緒に使用することをお勧めします `MergeTree`. 完全なデータを格納する `MergeTree` テーブルおよび使用 `SummingMergeTree` たとえば、レポートの準備時など、集計データを格納する場合。 このようなアプローチを防ぎまらな貴重なデータにより正しく構成しその有効なタイプを利用します。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = SummingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

説明リクエストパラメータの参照 [要求の説明](../../../sql-reference/statements/create.md).

**Sumingmergetreeのパラメータ**

-   `columns` -値が集計される列の名前を持つタプル。 任意パラメータ。
    列は数値型である必要があり、主キーには含まれていない必要があります。

    もし `columns` 指定されていないClickHouseは、主キーにない数値データ型を持つすべての列の値を集計します。

**クエリ句**

を作成するとき `SummingMergeTree` 同じテーブル [句](mergetree.md) を作成するときのように必要です。 `MergeTree` テーブル。

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
) ENGINE [=] SummingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
```

以下を除くすべてのパラメータ `columns` と同じ意味を持つ `MergeTree`.

-   `columns` — tuple with names of columns values of which will be summarized. Optional parameter. For a description, see the text above.

</details>

## 使用例 {#usage-example}

次の表を考えます:

``` sql
CREATE TABLE summtt
(
    key UInt32,
    value UInt32
)
ENGINE = SummingMergeTree()
ORDER BY key
```

データを挿入する:

``` sql
INSERT INTO summtt Values(1,1),(1,2),(2,1)
```

ClickHouseは完全ではないすべての行を合計可能性があります ([以下を参照](#data-processing)）ので、集計関数を使用します `sum` と `GROUP BY` クエリ内の句。

``` sql
SELECT key, sum(value) FROM summtt GROUP BY key
```

``` text
┌─key─┬─sum(value)─┐
│   2 │          1 │
│   1 │          3 │
└─────┴────────────┘
```

## データ処理 {#data-processing}

データがテーブルに挿入されると、そのまま保存されます。 ClickHouseは、データの挿入された部分を定期的にマージし、これは、同じ主キーを持つ行が合計され、結果として得られるデータの各部分に対して行に置き換えられる

ClickHouse can merge the data parts so that different resulting parts of data cat consist rows with the same primary key, i.e. the summation will be incomplete. Therefore (`SELECT`)集計関数 [和()](../../../sql-reference/aggregate-functions/reference.md#agg_function-sum) と `GROUP BY` 上記の例で説明したように、クエリで句を使用する必要があります。

### 合計の共通ルール {#common-rules-for-summation}

数値データ型の列の値が集計されます。 列のセットは、パラメータによって定義されます `columns`.

合計のすべての列の値が0の場合、行は削除されます。

Columnが主キーになく、集計されていない場合は、既存の値から任意の値が選択されます。

主キーの列の値は集計されません。

### Aggregatefunction列の合計 {#the-summation-in-the-aggregatefunction-columns}

の列に対して [AggregateFunctionタイプ](../../../sql-reference/data-types/aggregatefunction.md) ClickHouseとして振る舞うと考えられてい [AggregatingMergeTree](aggregatingmergetree.md) 機能に従って集計するエンジン。

### 入れ子構造 {#nested-structures}

テーブルでネストしたデータ構造と加工"と言われています。

入れ子になったテーブルの名前が `Map` また、以下の条件を満たす少なくとも二つの列が含まれています:

-   最初の列は数値です `(*Int*, Date, DateTime)` または文字列 `(String, FixedString)` それを呼びましょう `key`,
-   他の列は算術演算です `(*Int*, Float32/64)` それを呼びましょう `(values...)`,

次に、この入れ子になったテーブルは `key => (values...)` その行をマージするとき、二つのデータセットの要素は `key` 対応するの合計と `(values...)`.

例:

``` text
[(1, 100)] + [(2, 150)] -> [(1, 100), (2, 150)]
[(1, 100)] + [(1, 150)] -> [(1, 250)]
[(1, 100)] + [(1, 150), (2, 150)] -> [(1, 250), (2, 150)]
[(1, 100), (2, 150)] + [(1, -100)] -> [(2, 150)]
```

データを要求するときは、 [sumMap(キー、値)](../../../sql-reference/aggregate-functions/reference.md) の集合のための関数 `Map`.

入れ子になったデータ構造の場合、合計のために列のタプルにその列を指定する必要はありません。

[元の記事](https://clickhouse.com/docs/en/operations/table_engines/summingmergetree/) <!--hide-->
