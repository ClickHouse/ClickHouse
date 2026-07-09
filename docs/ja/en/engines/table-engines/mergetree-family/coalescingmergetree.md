---
description: 'CoalescingMergeTree は MergeTree エンジンを継承しています。主な特徴は、
  パーツのマージ時に各カラムの最後の非 NULL 値を自動的に保持できることです。'
sidebar_label: 'CoalescingMergeTree'
sidebar_position: 50
slug: /engines/table-engines/mergetree-family/coalescingmergetree
title: 'CoalescingMergeTree テーブルエンジン'
keywords: ['CoalescingMergeTree']
show_related_blogs: true
doc_type: 'reference'
---

:::note バージョン 25.6 以降で利用可能
このテーブルエンジンは、OSS と Cloud の両方でバージョン 25.6 以降から利用できます。
:::

このエンジンは [MergeTree](/ja/engines/table-engines/mergetree-family/mergetree) を継承しています。主な違いはデータパーツのマージ方法にあります。`CoalescingMergeTree` テーブルでは、ClickHouse は同じ主キー (より正確には同じ[ソートキー](../../../engines/table-engines/mergetree-family/mergetree.md)) を持つすべての行を、各カラムの最新の非 NULL 値を含む 1 行に置き換えます。

これによりカラムレベルの upsert が可能になり、行全体ではなく特定のカラムだけを更新できます。

`CoalescingMergeTree` は、キーカラム以外のカラムで Nullable 型を使用することを前提としています。カラムが Nullable でない場合の動作は、[ReplacingMergeTree](/ja/engines/table-engines/mergetree-family/replacingmergetree) と同じです。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = CoalescingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

リクエストパラメータの説明は、[リクエストの説明](../../../sql-reference/statements/create/table.md)を参照してください。

<div id="parameters-of-coalescingmergetree">
  ### CoalescingMergeTreeのパラメータ
</div>

<div id="columns">
  #### カラム
</div>

`columns` - 任意。値を結合する対象のカラム名を指定するタプルです。指定するカラムは、パーティションまたはソートキーに含まれていてはなりません。`columns` が指定されていない場合、ClickHouse はソートキーに含まれないすべてのカラムの値を結合します。

<div id="query-clauses">
  ### クエリの句
</div>

`CoalescingMergeTree` テーブルの作成時には、`MergeTree` テーブルの作成時と同じ[句](../../../engines/table-engines/mergetree-family/mergetree.md)が必要です。

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
  ) ENGINE [=] CoalescingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
  ```

  `columns` を除くすべてのパラメータは、`MergeTree` と同じ意味を持ちます。

  * `columns` — 値を合計するカラム名のタプルです。省略可能なパラメータです。詳細は上記を参照してください。
</details>

<div id="usage-example">
  ## 使用例
</div>

次のテーブルについて考えます。

```sql
CREATE TABLE test_table
(
    key UInt64,
    value_int Nullable(UInt32),
    value_string Nullable(String),
    value_date Nullable(Date)
)
ENGINE = CoalescingMergeTree()
ORDER BY key
```

そこへデータを挿入します:

```sql
INSERT INTO test_table VALUES(1, NULL, NULL, '2025-01-01'), (2, 10, 'test', NULL);
INSERT INTO test_table VALUES(1, 42, 'win', '2025-02-01');
INSERT INTO test_table(key, value_date) VALUES(2, '2025-02-01');
```

結果は次のようになります：

```sql
SELECT * FROM test_table ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   1 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-01-01 │
│   2 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-02-01 │
│   2 │        10 │ test         │       ᴺᵁᴸᴸ │
└─────┴───────────┴──────────────┴────────────┘
```

正確で最終的な結果を得るための推奨クエリ:

```sql
SELECT * FROM test_table FINAL ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   2 │        10 │ test         │ 2025-02-01 │
└─────┴───────────┴──────────────┴────────────┘
```

`FINAL` 修飾子を使用すると、ClickHouse はクエリ時にマージロジックを適用するため、各カラムの正しく集約された「最新」の値を確実に取得できます。これは、CoalescingMergeTree テーブルに対してクエリを実行する際の、最も安全で正確な方法です。

:::note

`GROUP BY` を使う方法では、基盤となるパーツが完全にマージされていない場合、誤った結果が返されることがあります。

```sql
SELECT key, last_value(value_int), last_value(value_string), last_value(value_date)  FROM test_table GROUP BY key; -- Not recommended.
```

:::

<div id="tuple-element-aggregation">
  ## Tuple 要素の集約
</div>

`allow_tuple_element_aggregation` 設定を有効にすると、`Tuple` カラムは再帰的にフラット化され、各リーフ要素がそれぞれ独立して coalescing の対象になります。これにより、複数のフィールドを 1 つの `Tuple` カラムに格納しつつ、マージ時に要素ごとに coalescing できるようになります。各 `Nullable` サブカラムは、それぞれ独立して最新の非 NULL 値を保持します。

フラット化されたサブカラムには、通常のカラムと同じルールが適用されます。

* ソートキーまたはパーティションキー内の `Tuple` に属するサブカラムは、coalescing の対象から除外されます。
* `columns` を指定した場合、一覧に含まれる `Tuple` カラムのサブカラムのみが coalescing されます。

:::note
この設定は変更できないため、テーブルの作成時に指定する必要があります。
:::

```sql
CREATE TABLE coalescing_tuples
(
    key UInt64,
    data Tuple(
        value_a Nullable(UInt64),
        value_b Nullable(String),
        nested Tuple(
            value_c Nullable(UInt64)
        )
    )
) ENGINE = CoalescingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO coalescing_tuples VALUES (1, (100, NULL, (NULL)));
INSERT INTO coalescing_tuples VALUES (1, (NULL, 'hello', (42)));

SELECT key, data.value_a, data.value_b, data.nested.value_c FROM coalescing_tuples FINAL;
```

```text
┌─key─┬─data.value_a─┬─data.value_b─┬─data.nested.value_c─┐
│   1 │          100 │ hello        │                  42 │
└─────┴──────────────┴──────────────┴─────────────────────┘
```