---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 34
toc_title: SummingMergeTree
---

# Summingmergetree {#summingmergetree}

エンジンは [MergeTree](mergetree.md#table_engines-mergetree). 違いは、データ部分をマージするとき `SummingMergeTree` テーブルClickHouseは、すべての行を同じ主キー（またはより正確には同じ）で置き換えます [ソートキー](mergetree.md))数値データ型を持つ列の集計値を含む行。 並べ替えキーが単一のキー値が多数の行に対応するように構成されている場合、これによりストレージボリュームが大幅に削減され、データ選択がスピードア

私たちは使用するエンジンと一緒に `MergeTree`. 完全なデータを格納する `MergeTree` テーブル、および使用 `SummingMergeTree` レポートを準備するときなど、集計データを保存する場合。 このようなアプローチは、誤って構成された主キーのために貴重なデー

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

**SummingMergeTreeのパラメータ**

-   `columns` -値が要約される列の名前を持つタプル。 省略可能なパラメータ。
    列は数値型である必要があり、主キーに含めることはできません。

    もし `columns` 指定されていない場合、ClickHouseは、プライマリキーに含まれていない数値データ型を持つすべての列の値を集計します。

**クエリ句**

作成するとき `SummingMergeTree` テーブル同じ [句](mergetree.md) 作成するときと同じように、必須です。 `MergeTree` テーブル。

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
) ENGINE [=] SummingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
```

すべてのパラメーターを除く `columns` と同じ意味を持つ `MergeTree`.

-   `columns` — tuple with names of columns values of which will be summarized. Optional parameter. For a description, see the text above.

</details>

## 使用例 {#usage-example}

次の表を考えてみます:

``` sql
CREATE TABLE summtt
(
    key UInt32,
    value UInt32
)
ENGINE = SummingMergeTree()
ORDER BY key
```

それにデータを挿入する:

``` sql
INSERT INTO summtt Values(1,1),(1,2),(2,1)
```

ClickHouseは完全ではないすべての行を合計してもよい ([以下を参照](#data-processing)）ので、我々は集計関数を使用します `sum` と `GROUP BY` クエリ内の句。

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

データがテーブルに挿入されると、そのまま保存されます。 これは、同じプライマリキーを持つ行が合計され、結果のデータ部分ごとに行が置き換えられたときです。

ClickHouse can merge the data parts so that different resulting parts of data cat consist rows with the same primary key, i.e. the summation will be incomplete. Therefore (`SELECT`)集計関数 [合計()](../../../sql-reference/aggregate-functions/reference.md#agg_function-sum) と `GROUP BY` 上記の例で説明したように、クエリで句を使用する必要があります。

### 合計の共通ルール {#common-rules-for-summation}

数値データ型の列の値が集計されます。 列のセットは、パラメータによって定義されます `columns`.

合計のすべての列の値が0の場合、行は削除されます。

列が主キーに含まれておらず、まとめられていない場合は、既存の値から任意の値が選択されます。

主キーの列の値は集計されません。

### Aggregatefunction列の合計 {#the-summation-in-the-aggregatefunction-columns}

列の場合 [AggregateFunctionタイプ](../../../sql-reference/data-types/aggregatefunction.md) クリックハウスは [ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂづｭﾂつｹ](aggregatingmergetree.md) 機能に従って集約するエンジン。

### 入れ子構造 {#nested-structures}

テーブルでネストしたデータ構造と加工"と言われています。

入れ子になったテーブルの名前が `Map` また、以下の条件を満たす少なくとも二つの列が含まれています:

-   最初の列は数値です `(*Int*, Date, DateTime)` または文字列 `(String, FixedString)`、それを呼びましょう `key`,
-   他の列は算術演算です `(*Int*, Float32/64)`、それを呼びましょう `(values...)`,

次に、このネストされたテーブルは、 `key => (values...)` 行をマージすると、二つのデータセットの要素は次のようにマージされます `key` 対応する `(values...)`.

例:

``` text
[(1, 100)] + [(2, 150)] -> [(1, 100), (2, 150)]
[(1, 100)] + [(1, 150)] -> [(1, 250)]
[(1, 100)] + [(1, 150), (2, 150)] -> [(1, 250), (2, 150)]
[(1, 100), (2, 150)] + [(1, -100)] -> [(2, 150)]
```

データを要求するときは、 [sumMap(キー,値)](../../../sql-reference/aggregate-functions/reference.md) の集約のための関数 `Map`.

入れ子になったデータ構造の場合、合計の列のタプルに列を指定する必要はありません。

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/summingmergetree/) <!--hide-->
