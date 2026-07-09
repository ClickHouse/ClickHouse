---
description: 'SummingMergeTree は MergeTree エンジンを継承しています。その主な特徴は、
  パーツのマージ時に数値データを自動的に合計できることです。'
sidebar_label: 'SummingMergeTree'
sidebar_position: 50
slug: /engines/table-engines/mergetree-family/summingmergetree
title: 'SummingMergeTree テーブルエンジン'
doc_type: 'reference'
---

このエンジンは [MergeTree](/ja/engines/table-engines/mergetree-family/mergetree) を継承しています。違いは、`SummingMergeTree` テーブルのデータパーツをマージする際に、ClickHouse が同じ主キー (より正確には同じ[ソートキー](../../../engines/table-engines/mergetree-family/mergetree.md)) を持つすべての行を、数値データ型のカラムの値を合計した 1 行に置き換える点です。ソートキーが、1 つのキー値に多数の行が対応するように構成されている場合、これによりストレージ容量を大幅に削減し、データの選択を高速化できます。

このエンジンは `MergeTree` と組み合わせて使用することを推奨します。完全なデータは `MergeTree` テーブルに保存し、`SummingMergeTree` は、たとえばレポート作成時などの集計データの保存に使用してください。このような方法を取ることで、不適切に構成された主キーによって貴重なデータが失われるのを防げます。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
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

リクエストパラメータの説明については、[リクエストの説明](../../../sql-reference/statements/create/table.md)を参照してください。

<div id="parameters-of-summingmergetree">
  ### SummingMergeTreeのパラメータ
</div>

<div id="columns">
  #### カラム
</div>

`columns` - 値を合計する対象となるカラム名のタプルです。省略可能なパラメーターです。
カラムは数値型である必要があり、パーティションキーまたはソートキーに含まれていてはなりません。

`columns` が指定されていない場合、ClickHouse はソートキーに含まれない数値データ型のすべてのカラムの値を合計します。

<div id="query-clauses">
  ### クエリ句
</div>

`SummingMergeTree` テーブルの作成時には、`MergeTree` テーブルの作成時と同じ[句](../../../engines/table-engines/mergetree-family/mergetree.md)が必要です。

<details markdown="1">
  <summary>非推奨のテーブル作成方法</summary>

  :::note
  新規プロジェクトではこの方法を使用しないでください。可能であれば、既存のプロジェクトも上記で説明した方法に切り替えてください。
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] SummingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
  ```

  `columns` を除くすべてのパラメータは、`MergeTree` の場合と同じ意味です。

  * `columns` — 合計対象とする値を持つカラム名のタプルです。これは任意のパラメータです。説明については、上記の本文を参照してください。
</details>

<div id="usage-example">
  ## 使用例
</div>

次のテーブルを見てみましょう。

```sql
CREATE TABLE summtt
(
    key UInt32,
    value UInt32
)
ENGINE = SummingMergeTree()
ORDER BY key
```

その中にデータを挿入します:

```sql
INSERT INTO summtt VALUES(1,1),(1,2),(2,1)
```

ClickHouse ではすべての行が完全には合計されない場合があるため ([以下を参照](#data-processing)) 、クエリでは集約関数 `sum` と `GROUP BY` 句を使用します。

```sql
SELECT key, sum(value) FROM summtt GROUP BY key
```

```text
┌─key─┬─sum(value)─┐
│   2 │          1 │
│   1 │          3 │
└─────┴────────────┘
```

<div id="data-processing">
  ## データ処理
</div>

データがテーブルに挿入されると、そのまま保存されます。ClickHouse は挿入されたデータパーツを定期的にマージし、その際に同じ主キーを持つ行が合計され、マージ後の各データパーツでは 1 行にまとめられます。

ただし、ClickHouse ではデータパーツが別々にマージされることがあるため、マージ後の異なるデータパーツに同じ主キーを持つ行が残り、合計が不完全になる場合があります。したがって、上記の例で説明したように、クエリでは (`SELECT`) 集約関数 [sum()](/ja/sql-reference/aggregate-functions/reference/sum) と `GROUP BY` 句を使用する必要があります。

<div id="common-rules-for-summation">
  ### 合計に関する一般的なルール
</div>

数値データ型のカラム内の値は合計されます。対象となるカラムのセットは、パラメータ `columns` で定義されます。

合計対象のすべてのカラムの値が 0 の場合、その行は削除されます。

カラムが主キーに含まれておらず、かつ合計対象でない場合は、既存の値の中から任意の値が選択されます。

主キーに含まれるカラムの値は合計されません。

<div id="the-summation-in-the-aggregatefunction-columns">
  ### AggregateFunction カラムにおける合計
</div>

[AggregateFunction 型](../../../sql-reference/data-types/aggregatefunction.md)のカラムに対して、ClickHouse は関数に応じて集約を行う [AggregatingMergeTree](../../../engines/table-engines/mergetree-family/aggregatingmergetree.md) エンジンのように動作します。

<div id="nested-structures">
  ### ネストされた構造
</div>

テーブルには、特別な方法で処理されるネストされたデータ構造を含めることができます。

ネストされたテーブルの名前が `Map` で終わり、さらに次の条件を満たすカラムを少なくとも 2 つ含む場合:

* 最初のカラムが数値型 `(*Int*, Date, DateTime)` または文字列型 `(String, FixedString)` であるものを `key` とします。
* それ以外のカラムが算術型 `(*Int*, Float32/64)` であるものを `(values...)` とします。

このネストされたテーブルは `key => (values...)` のマッピングとして解釈され、行のマージ時には、2 つの data set の要素が `key` ごとにマージされ、対応する `(values...)` は合計されます。

例:

```text
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

OPTIMIZE TABLE nested_sum FINAL; -- emulate merge 

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

データを取得する際は、`Map` の集計に [sumMap(key, value)](../../../sql-reference/aggregate-functions/reference/sumMappedArrays.md) 関数を使用します。

ネストされたデータ構造では、合計対象のカラムのタプルにそのカラムを指定する必要はありません。

<div id="tuple-element-aggregation">
  ### Tuple 要素の集約
</div>

`allow_tuple_element_aggregation` 設定を有効にすると、`Tuple` カラムは再帰的にフラット化され、各リーフ要素がそれぞれ独立して合計の対象になります。これにより、複数のメトリクスを 1 つの `Tuple` カラムに格納し、マージ時に要素ごとに合計できるようになります。

フラット化されたサブカラムには、通常のカラムと同じルールが適用されます。

* 合計されるのは数値サブカラムのみです。
* ソートキー または パーティションキー に含まれる `Tuple` に属するサブカラムは、合計対象から除外されます。
* `columns` が指定されている場合、列挙された `Tuple` カラムのサブカラムのみが合計されます。
* 合計後、ある行のすべての数値サブカラムが 0 になった場合、その行は削除されます。

:::note
この設定は変更不可であり、テーブル作成時に指定する必要があります。
:::

```sql
CREATE TABLE summing_tuples
(
    key UInt32,
    metrics Tuple(
        impressions UInt64,
        clicks UInt64,
        nested Tuple(
            conversions UInt64
        )
    )
) ENGINE = SummingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO summing_tuples VALUES (1, (100, 10, (1)));
INSERT INTO summing_tuples VALUES (1, (200, 20, (3)));

OPTIMIZE TABLE summing_tuples FINAL;

SELECT key, metrics.impressions, metrics.clicks, metrics.nested.conversions FROM summing_tuples;
```

```text
┌─key─┬─metrics.impressions─┬─metrics.clicks─┬─metrics.nested.conversions─┐
│   1 │                 300 │             30 │                          4 │
└─────┴─────────────────────┴────────────────┴────────────────────────────┘
```

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouseでArray、Map、stateに集約関数コンビネータを使用する](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)