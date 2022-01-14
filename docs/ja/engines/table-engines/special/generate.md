---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: GenerateRandom
---

# Generaterandom {#table_engines-generate}

のGenerateRandomテーブルエンジンの生産ランダムなデータが与えられたテーブルのスキーマ.

使用例:

-   再現可能で大きいテーブルを移入するテストの使用。
-   ファジングテストのランダム入力を生成します。

## ClickHouseサーバーでの使用状況 {#usage-in-clickhouse-server}

``` sql
ENGINE = GenerateRandom(random_seed, max_string_length, max_array_length)
```

その `max_array_length` と `max_string_length` パラメータを指定し最大限の長さのすべて
生成されたデータに対応する配列の列と文字列。

Generate table engineのサポートのみ `SELECT` クエリ。

それはすべて [データ型](../../../sql-reference/data-types/index.md) を除いてテーブルに格納することができます `LowCardinality` と `AggregateFunction`.

**例:**

**1.** セットアップ `generate_engine_table` テーブル:

``` sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)
```

**2.** データの照会:

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

## 実施内容 {#details-of-implementation}

-   対応していません:
    -   `ALTER`
    -   `SELECT ... SAMPLE`
    -   `INSERT`
    -   指数
    -   複製

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/generate/) <!--hide-->
