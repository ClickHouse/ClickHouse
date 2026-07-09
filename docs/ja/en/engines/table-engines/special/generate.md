---
description: 'GenerateRandom テーブルエンジンは、指定したテーブルスキーマに基づいてランダムデータを生成します。'
sidebar_label: 'GenerateRandom'
sidebar_position: 140
slug: /engines/table-engines/special/generate
title: 'GenerateRandom テーブルエンジン'
doc_type: 'reference'
---

GenerateRandom テーブルエンジンは、指定したテーブルスキーマに基づいてランダムデータを生成します。

使用例:

* テストで、再現可能な大規模テーブルにデータを投入するために使用します。
* ファジングテスト用のランダムな入力を生成します。

<div id="usage-in-clickhouse-server">
  ## ClickHouse Serverでの使用状況
</div>

```sql
ENGINE = GenerateRandom([random_seed [,max_string_length [,max_array_length]]])
```

`max_array_length` および `max_string_length` パラメータは、生成されるデータ内のすべての
`Array` または `Map` 型のカラムと文字列について、それぞれの最大長を指定します。

Generate テーブルエンジンがサポートするのは `SELECT` クエリのみです。

`AggregateFunction` を除き、テーブルに格納できるすべての [データ型](../../../sql-reference/data-types/index.md) をサポートします。

<div id="example">
  ## 例
</div>

**1.** `generate_engine_table` テーブルを設定します:

```sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)
```

**2.** データをクエリする：

```sql
SELECT * FROM generate_engine_table LIMIT 3
```

```text
┌─name─┬──────value─┐
│ c4xJ │ 1412771199 │
│ r    │ 1791099446 │
│ 7#$  │  124312908 │
└──────┴────────────┘
```

<div id="details-of-implementation">
  ## 実装の詳細
</div>

* 以下はサポートされていません:
  * `ALTER`
  * `SELECT ... SAMPLE`
  * `INSERT`
  * インデックス
  * レプリケーション