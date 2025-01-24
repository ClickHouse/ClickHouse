---
slug: /ja/engines/table-engines/special/generate
sidebar_position: 140
sidebar_label: GenerateRandom
title: "GenerateRandom テーブルエンジン"
---

GenerateRandomテーブルエンジンは、指定したテーブルスキーマに対してランダムなデータを生成します。

使用例:

- テストで再現可能な大規模テーブルを生成する。
- ファジングテストのためにランダムな入力を生成する。

## ClickHouseサーバーでの使用例 {#usage-in-clickhouse-server}

``` sql
ENGINE = GenerateRandom([random_seed [,max_string_length [,max_array_length]]])
```

`max_array_length`と`max_string_length`パラメータは、生成されたデータ内のすべての配列またはマップカラムと文字列の最大長をそれぞれ指定します。

Generateテーブルエンジンは`SELECT`クエリのみをサポートします。

表に格納できるすべての[データ型](../../../sql-reference/data-types/index.md)をサポートし、`AggregateFunction`を除きます。

## 例 {#example}

**1.** `generate_engine_table` テーブルをセットアップする:

``` sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)
```

**2.** データをクエリする:

``` sql
SELECT * FROM generate_engine_table LIMIT 3
```

``` text
┌─name─┬──────value─┐
│ c4xJ │ 1412771199 │
│ r    │ 1791099446 │
│ 7#$  │  124312908 │
└──────┴────────────┘
```

## 実装の詳細 {#details-of-implementation}

- サポートされていない機能:
    - `ALTER`
    - `SELECT ... SAMPLE`
    - `INSERT`
    - インデックス
    - レプリケーション
