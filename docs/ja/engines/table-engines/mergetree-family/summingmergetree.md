---
slug: /ja/engines/table-engines/mergetree-family/summingmergetree
sidebar_position: 50
sidebar_label: SummingMergeTree
---

# SummingMergeTree

このエンジンは、[MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engines-mergetree)から継承されています。違いは、`SummingMergeTree`テーブルにおいてデータパーツをマージする際、ClickHouseはすべての主キー（より正確には[ソートキー](../../../engines/table-engines/mergetree-family/mergetree.md)）が同じ行を数値データ型のカラムにおける要約値を含む一つの行で置き換えることです。単一のキーの値が多くの行に対応するようなソートキーによって構成されている場合、これはストレージの容量を大幅に削減し、データ選択を高速化します。

エンジンを`MergeTree`と共に使用することをお勧めします。完全なデータは`MergeTree`テーブルに保存し、集約データの保存には`SummingMergeTree`を使用します。例えば、レポートを作成する際にこのようなアプローチを採用することで、誤った主キーの設定による貴重なデータの損失を防ぎます。

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

リクエストパラメータの詳細については[リクエストの説明](../../../sql-reference/statements/create/table.md)を参照してください。

### SummingMergeTreeのパラメータ

#### columns

`columns` - 要約される値のカラム名のタプル。オプションのパラメータ。
カラムは数値型であり、主キーに含まれてはなりません。

`columns`が指定されていない場合、ClickHouseは主キーに含まれていないすべての数値データ型のカラムの値を要約します。

### クエリ句

`SummingMergeTree` テーブルを作成する際には、`MergeTree` テーブルを作成する場合と同じ[句](../../../engines/table-engines/mergetree-family/mergetree.md)が必要です。

<details markdown="1">

<summary>非推奨のテーブル作成方法</summary>

:::note
新しいプロジェクトではこの方法を使用せず、可能なら既存のプロジェクトを上記の方法に変更してください。
:::

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] SummingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
```

`columns`を除くすべてのパラメータは、`MergeTree`における意味と同じです。

- `columns` — 要約される値のカラム名のタプル。オプションのパラメータ。説明については上記を参照してください。

</details>

## 使用例 {#usage-example}

次のテーブルを考えます:

``` sql
CREATE TABLE summtt
(
    key UInt32,
    value UInt32
)
ENGINE = SummingMergeTree()
ORDER BY key
```

データを挿入します:

``` sql
INSERT INTO summtt Values(1,1),(1,2),(2,1)
```

ClickHouseはすべての行を完全には合計しない場合があります（[以下を参照](#data-processing)）、そのためクエリ内で集計関数`sum`と`GROUP BY`句を使用します。

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

データがテーブルに挿入されると、そのまま保存されます。ClickHouseは定期的にデータの挿入部分をマージし、このとき同じ主キーを持つ行が合計され、それぞれの最終的なデータ部分に対して一つの行に置き換えられます。

ClickHouseはデータ部分をマージして異なる結果データ部分に同じ主キーを持つ行を含めることができ、つまり合計が不完全になることがあります。したがって、クエリ内では集計関数[sum()](../../../sql-reference/aggregate-functions/reference/sum.md#agg_function-sum)および`GROUP BY`句を使用するべきです。

### 要約の共通ルール {#common-rules-for-summation}

数値データ型のカラムでは値が要約されます。カラムの集合は`columns`パラメータによって定義されます。

要約のすべてのカラムで値が0だった場合、行は削除されます。

主キーに含まれておらず、要約もされていないカラムについては、既存の値から任意のものが選ばれます。

主キー内のカラムに対しては値は要約されません。

### 集約関数カラム内の要約 {#the-summation-in-the-aggregatefunction-columns}

[AggregateFunction型](../../../sql-reference/data-types/aggregatefunction.md)のカラムについて、ClickHouseは[AggregatingMergeTree](../../../engines/table-engines/mergetree-family/aggregatingmergetree.md)エンジンのように関数に従って集約します。

### ネストした構造 {#nested-structures}

テーブルは特別な方法で処理されるネストしたデータ構造を持つことができます。

ネストしたテーブルの名前が`Map`で終わり、以下の基準を満たす少なくとも2つのカラムを含んでいる場合：

- 最初のカラムが数値型`(*Int*, Date, DateTime)`または文字列型`(String, FixedString)`、これを`key`と呼びます、
- 他のカラムが算術型`(*Int*, Float32/64)`、これを`(values...)`と呼びます、

ネストしたテーブルは`key => (values...)`のマッピングとして解釈され、その行をマージする際に2つのデータセットの要素が`key`によってマージされ、対応する`(values...)`が合計されます。

例：

``` text
DROP TABLE IF EXISTS nested_sum;
CREATE TABLE nested_sum
(
    date Date,
    site UInt32,
    hitsMap Nested(
        browser String,
        imps UInt32,
        clicks UInt32
    )
) ENGINE = SummingMergeTree
PRIMARY KEY (date, site);

INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['Firefox', 'Opera'], [10, 5], [2, 1]);
INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['Chrome', 'Firefox'], [20, 1], [1, 1]);
INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['IE'], [22], [0]);
INSERT INTO nested_sum VALUES ('2020-01-01', 10, ['Chrome'], [4], [3]);

OPTIMIZE TABLE nested_sum FINAL; -- マージをエミュレート

SELECT * FROM nested_sum;
┌───────date─┬─site─┬─hitsMap.browser───────────────────┬─hitsMap.imps─┬─hitsMap.clicks─┐
│ 2020-01-01 │   10 │ ['Chrome']                        │ [4]          │ [3]            │
│ 2020-01-01 │   12 │ ['Chrome','Firefox','IE','Opera'] │ [20,11,22,5] │ [1,3,0,1]      │
└────────────┴──────┴───────────────────────────────────┴──────────────┴────────────────┘

SELECT
    site,
    browser,
    impressions,
    clicks
FROM
(
    SELECT
        site,
        sumMap(hitsMap.browser, hitsMap.imps, hitsMap.clicks) AS imps_map
    FROM nested_sum
    GROUP BY site
)
ARRAY JOIN
    imps_map.1 AS browser,
    imps_map.2 AS impressions,
    imps_map.3 AS clicks;

┌─site─┬─browser─┬─impressions─┬─clicks─┐
│   12 │ Chrome  │          20 │      1 │
│   12 │ Firefox │          11 │      3 │
│   12 │ IE      │          22 │      0 │
│   12 │ Opera   │           5 │      1 │
│   10 │ Chrome  │           4 │      3 │
└──────┴─────────┴─────────────┴────────┘
```

データの要求時に、`Map`の集計には[sumMap(key, value)](../../../sql-reference/aggregate-functions/reference/summap.md)関数を使用してください。

ネストしたデータ構造の場合、そのカラムを要約のためのタプル内に指定する必要はありません。

## 関連コンテンツ

- ブログ: [Using Aggregate Combinators in ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
