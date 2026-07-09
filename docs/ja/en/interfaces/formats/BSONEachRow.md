---
alias: []
description: 'BSONEachRow フォーマットに関するドキュメント'
input_format: true
keywords: ['BSONEachRow']
output_format: true
slug: /interfaces/formats/BSONEachRow
title: 'BSONEachRow'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

`BSONEachRow`フォーマットは、Binary JSON (BSON) ドキュメントが区切りなく連続する形式のデータを解析します。
各行は1つのドキュメントとしてフォーマットされ、各カラムはカラム名をキーとする1つのBSONドキュメントフィールドとしてフォーマットされます。

<div id="data-types-matching">
  ## データ型の対応
</div>

出力では、ClickHouse 型と BSON 型の対応は次のとおりです。

| ClickHouse 型                                                                                          | BSON Type                                                                                                                      |
| ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| [Bool](/ja/sql-reference/data-types/boolean.md)                                                          | `\x08` boolean                                                                                                                 |
| [Int8/UInt8](/ja/sql-reference/data-types/int-uint.md)/[Enum8](/ja/sql-reference/data-types/enum.md)        | `\x10` int32                                                                                                                   |
| [Int16/UInt16](/ja/sql-reference/data-types/int-uint.md)/[Enum16](/ja/sql-reference/data-types/enum.md)     | `\x10` int32                                                                                                                   |
| [Int32](/ja/sql-reference/data-types/int-uint.md)                                                        | `\x10` int32                                                                                                                   |
| [UInt32](/ja/sql-reference/data-types/int-uint.md)                                                       | `\x12` int64                                                                                                                   |
| [Int64/UInt64](/ja/sql-reference/data-types/int-uint.md)                                                 | `\x12` int64                                                                                                                   |
| [Float32/Float64](/ja/sql-reference/data-types/float.md)                                                 | `\x01` double                                                                                                                  |
| [Date](/ja/sql-reference/data-types/date.md)/[Date32](/ja/sql-reference/data-types/date32.md)               | `\x10` int32                                                                                                                   |
| [DateTime](/ja/sql-reference/data-types/datetime.md)                                                     | `\x12` int64                                                                                                                   |
| [DateTime64](/ja/sql-reference/data-types/datetime64.md)                                                 | `\x09` datetime                                                                                                                |
| [Decimal32](/ja/sql-reference/data-types/decimal.md)                                                     | `\x10` int32                                                                                                                   |
| [Decimal64](/ja/sql-reference/data-types/decimal.md)                                                     | `\x12` int64                                                                                                                   |
| [Decimal128](/ja/sql-reference/data-types/decimal.md)                                                    | `\x05` binary, `\x00` binary subtype, size = 16                                                                                |
| [Decimal256](/ja/sql-reference/data-types/decimal.md)                                                    | `\x05` binary, `\x00` binary subtype, size = 32                                                                                |
| [Int128/UInt128](/ja/sql-reference/data-types/int-uint.md)                                               | `\x05` binary, `\x00` binary subtype, size = 16                                                                                |
| [Int256/UInt256](/ja/sql-reference/data-types/int-uint.md)                                               | `\x05` binary, `\x00` binary subtype, size = 32                                                                                |
| [String](/ja/sql-reference/data-types/string.md)/[FixedString](/ja/sql-reference/data-types/fixedstring.md) | `\x05` binary, `\x00` binary subtype、または setting output&#95;format&#95;bson&#95;string&#95;as&#95;string が有効な場合は `\x02` string |
| [UUID](/ja/sql-reference/data-types/uuid.md)                                                             | `\x05` binary, `\x04` uuid subtype, size = 16                                                                                  |
| [Array](/ja/sql-reference/data-types/array.md)                                                           | `\x04` array                                                                                                                   |
| [Tuple](/ja/sql-reference/data-types/tuple.md)                                                           | `\x04` array                                                                                                                   |
| [Named Tuple](/ja/sql-reference/data-types/tuple.md)                                                     | `\x03` document                                                                                                                |
| [Map](/ja/sql-reference/data-types/map.md)                                                               | `\x03` document                                                                                                                |
| [IPv4](/ja/sql-reference/data-types/ipv4.md)                                                             | `\x10` int32                                                                                                                   |
| [IPv6](/ja/sql-reference/data-types/ipv6.md)                                                             | `\x05` binary, `\x00` binary subtype                                                                                           |

入力では、BSON 型と ClickHouse 型の対応は次のとおりです。

| BSON Type                                | ClickHouse Type                                                                                                                                                                                     |
| ---------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `\x01` double                            | [Float32/Float64](/ja/sql-reference/data-types/float.md)                                                                                                                                               |
| `\x02` string                            | [String](/ja/sql-reference/data-types/string.md)/[FixedString](/ja/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x03` document                          | [Map](/ja/sql-reference/data-types/map.md)/[Named Tuple](/ja/sql-reference/data-types/tuple.md)                                                                                                           |
| `\x04` array                             | [Array](/ja/sql-reference/data-types/array.md)/[Tuple](/ja/sql-reference/data-types/tuple.md)                                                                                                             |
| `\x05` binary, `\x00` binary subtype     | [String](/ja/sql-reference/data-types/string.md)/[FixedString](/ja/sql-reference/data-types/fixedstring.md)/[IPv6](/ja/sql-reference/data-types/ipv6.md)                                                     |
| `\x05` binary, `\x02` old binary subtype | [String](/ja/sql-reference/data-types/string.md)/[FixedString](/ja/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x05` binary, `\x03` old uuid subtype   | [UUID](/ja/sql-reference/data-types/uuid.md)                                                                                                                                                           |
| `\x05` binary, `\x04` uuid subtype       | [UUID](/ja/sql-reference/data-types/uuid.md)                                                                                                                                                           |
| `\x07` ObjectId                          | [String](/ja/sql-reference/data-types/string.md)/[FixedString](/ja/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x08` boolean                           | [Bool](/ja/sql-reference/data-types/boolean.md)                                                                                                                                                        |
| `\x09` datetime                          | [DateTime64](/ja/sql-reference/data-types/datetime64.md)                                                                                                                                               |
| `\x0A` null value                        | [NULL](/ja/sql-reference/data-types/nullable.md)                                                                                                                                                       |
| `\x0D` JavaScript code                   | [String](/ja/sql-reference/data-types/string.md)/[FixedString](/ja/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x0E` symbol                            | [String](/ja/sql-reference/data-types/string.md)/[FixedString](/ja/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x10` int32                             | [Int32/UInt32](/ja/sql-reference/data-types/int-uint.md)/[Decimal32](/ja/sql-reference/data-types/decimal.md)/[IPv4](/ja/sql-reference/data-types/ipv4.md)/[Enum8/Enum16](/ja/sql-reference/data-types/enum.md) |
| `\x12` int64                             | [Int64/UInt64](/ja/sql-reference/data-types/int-uint.md)/[Decimal64](/ja/sql-reference/data-types/decimal.md)/[DateTime64](/ja/sql-reference/data-types/datetime64.md)                                       |

その他の BSON 型はサポートされていません。また、異なる整数型間の変換も行われます。
たとえば、BSON の `int32` 値を ClickHouse に [`UInt8`](../../sql-reference/data-types/int-uint.md) として挿入できます。

`Int128`/`UInt128`/`Int256`/`UInt256`/`Decimal128`/`Decimal256` などの大きな整数や Decimal は、`\x00` binary subtype を持つ BSON Binary 値からパースできます。
この場合、このフォーマットはバイナリデータのサイズが期待される値のサイズと等しいことを検証します。

:::note
このフォーマットは、ビッグエンディアンのプラットフォームでは正しく動作しません。
:::

<div id="example-usage">
  ## 使用例
</div>

<div id="inserting-data">
  ### データの挿入
</div>

以下のデータを含む `football.bson` という名前の BSON ファイルを使用します。

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
INSERT INTO football FROM INFILE 'football.bson' FORMAT BSONEachRow;
```

<div id="reading-data">
  ### データの読み込み
</div>

`BSONEachRow`フォーマットを使用してデータを読み込みます：

```sql
SELECT *
FROM football INTO OUTFILE 'docs_data/bson/football.bson'
FORMAT BSONEachRow
```

:::tip
BSON はバイナリ形式のため、ターミナルには可読な形では表示されません。BSON ファイルを出力するには、`INTO OUTFILE` を使用してください。
:::

<div id="format-settings">
  ## フォーマット設定
</div>

| 設定                                                                                                                                                                                                    | 説明                                                    | 既定値     |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------- | ------- |
| [`output_format_bson_string_as_string`](../../operations/settings/settings-formats.md/#output_format_bson_string_as_string)                                                                           | String 型のカラムでは、Binary の代わりに BSON String 型を使用します。      | `false` |
| [`input_format_bson_skip_fields_with_unsupported_types_in_schema_inference`](../../operations/settings/settings-formats.md/#input_format_bson_skip_fields_with_unsupported_types_in_schema_inference) | BSONEachRow フォーマットのスキーマ推論時に、サポートされていない型のカラムをスキップできます。 | `false` |