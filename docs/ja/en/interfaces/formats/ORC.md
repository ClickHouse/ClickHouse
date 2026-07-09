---
alias: []
description: 'ORCフォーマットのドキュメント'
input_format: true
keywords: ['ORC']
output_format: true
slug: /interfaces/formats/ORC
title: 'ORC'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

[Apache ORC](https://orc.apache.org/) は、[Hadoop](https://hadoop.apache.org/) エコシステムで広く使用されている列指向のストレージ形式です。

<div id="data-types-matching-orc">
  ## ORC のデータ型の対応
</div>

以下の表は、サポートされている ORC のデータ型と、`INSERT` クエリおよび `SELECT` クエリにおける対応する ClickHouse の [データ型](/ja/sql-reference/data-types/index.md) を比較したものです。

| ORC データ型 (`INSERT`)                   | ClickHouse データ型                                                                                   | ORC データ型 (`SELECT`)  |
| ------------------------------------- | ------------------------------------------------------------------------------------------------- | -------------------- |
| `Boolean`                             | [UInt8](/ja/sql-reference/data-types/int-uint.md)                                                    | `Boolean`            |
| `Tinyint`                             | [Int8/UInt8](/ja/sql-reference/data-types/int-uint.md)/[Enum8](/ja/sql-reference/data-types/enum.md)    | `Tinyint`            |
| `Smallint`                            | [Int16/UInt16](/ja/sql-reference/data-types/int-uint.md)/[Enum16](/ja/sql-reference/data-types/enum.md) | `Smallint`           |
| `Int`                                 | [Int32/UInt32](/ja/sql-reference/data-types/int-uint.md)                                             | `Int`                |
| `Bigint`                              | [Int64/UInt32](/ja/sql-reference/data-types/int-uint.md)                                             | `Bigint`             |
| `Float`                               | [Float32](/ja/sql-reference/data-types/float.md)                                                     | `Float`              |
| `Double`                              | [Float64](/ja/sql-reference/data-types/float.md)                                                     | `Double`             |
| `Decimal`                             | [Decimal](/ja/sql-reference/data-types/decimal.md)                                                   | `Decimal`            |
| `Date`                                | [Date32](/ja/sql-reference/data-types/date32.md)                                                     | `Date`               |
| `Timestamp`                           | [DateTime64](/ja/sql-reference/data-types/datetime64.md)                                             | `Timestamp`          |
| `String`, `Char`, `Varchar`, `Binary` | [String](/ja/sql-reference/data-types/string.md)                                                     | `Binary`             |
| `List`                                | [Array](/ja/sql-reference/data-types/array.md)                                                       | `List`               |
| `Struct`                              | [Tuple](/ja/sql-reference/data-types/tuple.md)                                                       | `Struct`             |
| `Map`                                 | [Map](/ja/sql-reference/data-types/map.md)                                                           | `Map`                |
| `Int`                                 | [IPv4](/ja/sql-reference/data-types/int-uint.md)                                                     | `Int`                |
| `Binary`                              | [IPv6](/ja/sql-reference/data-types/ipv6.md)                                                         | `Binary`             |
| `Binary`                              | [Int128/UInt128/Int256/UInt256](/ja/sql-reference/data-types/int-uint.md)                            | `Binary`             |
| `Binary`                              | [Decimal256](/ja/sql-reference/data-types/decimal.md)                                                | `Binary`             |

* その他の型はサポートされていません。
* Array はネスト可能で、引数として `Nullable` 型の値を取ることもできます。`Tuple` 型および `Map` 型もネストできます。
* ClickHouseテーブルのカラムのデータ型は、対応する ORC のデータフィールドと一致している必要はありません。データを挿入する際、ClickHouse はまず上記の表に従ってデータ型を解釈し、その後、そのデータを ClickHouseテーブルのカラムに設定されたデータ型へ [CAST](/ja/sql-reference/functions/type-conversion-functions#CAST) します。

<div id="example-usage">
  ## 使用例
</div>

<div id="inserting-data">
  ### データの挿入
</div>

以下のデータを含む `football.orc` という名前の ORC ファイルを使用します：

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

データを挿入します:

```sql
INSERT INTO football FROM INFILE 'football.orc' FORMAT ORC;
```

<div id="reading-data">
  ### データの読み込み
</div>

`ORC`フォーマットを使用してデータを読み込みます:

```sql
SELECT *
FROM football
INTO OUTFILE 'football.orc'
FORMAT ORC
```

:::tip
ORC はバイナリ形式のため、ターミナル上で人間が読める形では表示されません。ORC ファイルを出力するには、`INTO OUTFILE` を使用してください。
:::

<div id="format-settings">
  ## フォーマット設定
</div>

| 設定                                                                                                                                                                                                   | 説明                                                 | デフォルト   |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- | ------- |
| [`output_format_arrow_string_as_string`](/ja/operations/settings/settings-formats.md/#output_format_arrow_string_as_string)                                                                             | String 型のカラムでは、Binary ではなく Arrow String 型を使用します。   | `false` |
| [`output_format_orc_compression_method`](/ja/operations/settings/settings-formats.md/#output_format_orc_compression_method)                                                                             | 出力 ORC フォーマットで使用する圧縮方式。デフォルト値。                     | `none`  |
| [`input_format_arrow_case_insensitive_column_matching`](/ja/operations/settings/settings-formats.md/#input_format_arrow_case_insensitive_column_matching)                                               | Arrow のカラムと ClickHouse のカラムの照合時に、大文字と小文字を区別しません。   | `false` |
| [`input_format_arrow_allow_missing_columns`](/ja/operations/settings/settings-formats.md/#input_format_arrow_allow_missing_columns)                                                                     | Arrow データの読み取り時に、欠落しているカラムを許可します。                  | `false` |
| [`input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`](/ja/operations/settings/settings-formats.md/#input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference) | Arrow フォーマットのスキーマ推論時に、未サポートの型を持つカラムをスキップできるようにします。 | `false` |

Hadoop とデータをやり取りするには、[HDFS テーブルエンジン](/ja/engines/table-engines/integrations/hdfs.md)を使用できます。