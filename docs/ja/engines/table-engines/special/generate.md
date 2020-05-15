---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 46
toc_title: GenerateRandom
---

# Generaterandom {#table_engines-generate}

のgeneraterandomテーブルエンジンの生産ランダムなデータが与えられたテーブルのスキーマ.

使用例:

-   再現可能な大きいテーブルに住むテストの使用。
-   ファジングテストのランダム入力を生成します。

## Clickhouseサーバーでの使用状況 {#usage-in-clickhouse-server}

``` sql
ENGINE = GenerateRandom(random_seed, max_string_length, max_array_length)
```

その `max_array_length` と `max_string_length` すべ
生成されたデータに対応する配列の列と文字列。

テーブル生成エンジンは `SELECT` クエリ。

対応して [データタイプ](../../../sql-reference/data-types/index.md) これは、以下を除いてテーブルに格納できます `LowCardinality` と `AggregateFunction`.

**例えば:**

**1.** セットアップ `generate_engine_table` テーブル:

``` sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)
```

**2.** データのクエリ:

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

-   サポートなし:
    -   `ALTER`
    -   `SELECT ... SAMPLE`
    -   `INSERT`
    -   指数
    -   複製

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/generate/) <!--hide-->
