---
alias: []
description: 'MsgPackフォーマットのドキュメント'
input_format: true
keywords: ['MsgPack']
output_format: true
slug: /interfaces/formats/MsgPack
title: 'MsgPack'
doc_type: 'reference'
---

| 入力 | 出力 | 別名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 説明
</div>

ClickHouse は、[MessagePack](https://msgpack.org/)形式のデータファイルの読み書きをサポートしています。

<div id="data-types-matching">
  ## データ型の対応
</div>

| MessagePackのデータ型 (`INSERT`)                                        | ClickHouseのデータ型                                                                             | MessagePackのデータ型 (`SELECT`)        |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------- | ---------------------------------- |
| `uint N`, `positive fixint`                                        | [`UIntN`](/ja/sql-reference/data-types/int-uint.md)                                            | `uint N`                           |
| `int N`, `negative fixint`                                         | [`IntN`](/ja/sql-reference/data-types/int-uint.md)                                             | `int N`                            |
| `bool`                                                             | [`UInt8`](/ja/sql-reference/data-types/int-uint.md)                                            | `uint 8`                           |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`String`](/ja/sql-reference/data-types/string.md)                                             | `bin 8`, `bin 16`, `bin 32`        |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`FixedString`](/ja/sql-reference/data-types/fixedstring.md)                                   | `bin 8`, `bin 16`, `bin 32`        |
| `float 32`                                                         | [`Float32`](/ja/sql-reference/data-types/float.md)                                             | `float 32`                         |
| `float 64`                                                         | [`Float64`](/ja/sql-reference/data-types/float.md)                                             | `float 64`                         |
| `uint 16`                                                          | [`Date`](/ja/sql-reference/data-types/date.md)                                                 | `uint 16`                          |
| `int 32`                                                           | [`Date32`](/ja/sql-reference/data-types/date32.md)                                             | `int 32`                           |
| `uint 32`                                                          | [`DateTime`](/ja/sql-reference/data-types/datetime.md)                                         | `uint 32`                          |
| `uint 64`                                                          | [`DateTime64`](/ja/sql-reference/data-types/datetime.md)                                       | `uint 64`                          |
| `fixarray`, `array 16`, `array 32`                                 | [`Array`](/ja/sql-reference/data-types/array.md)/[`Tuple`](/ja/sql-reference/data-types/tuple.md) | `fixarray`, `array 16`, `array 32` |
| `fixmap`, `map 16`, `map 32`                                       | [`Map`](/ja/sql-reference/data-types/map.md)                                                   | `fixmap`, `map 16`, `map 32`       |
| `uint 32`                                                          | [`IPv4`](/ja/sql-reference/data-types/ipv4.md)                                                 | `uint 32`                          |
| `bin 8`                                                            | [`String`](/ja/sql-reference/data-types/string.md)                                             | `bin 8`                            |
| `int 8`                                                            | [`Enum8`](/ja/sql-reference/data-types/enum.md)                                                | `int 8`                            |
| `bin 8`                                                            | [`(U)Int128`/`(U)Int256`](/ja/sql-reference/data-types/int-uint.md)                            | `bin 8`                            |
| `int 32`                                                           | [`Decimal32`](/ja/sql-reference/data-types/decimal.md)                                         | `int 32`                           |
| `int 64`                                                           | [`Decimal64`](/ja/sql-reference/data-types/decimal.md)                                         | `int 64`                           |
| `bin 8`                                                            | [`Decimal128`/`Decimal256`](/ja/sql-reference/data-types/decimal.md)                           | `bin 8 `                           |

<div id="example-usage">
  ## 使用例
</div>

ファイル &quot;.msgpk&quot; への書き込み:

```sql
$ clickhouse-client --query="CREATE TABLE msgpack (array Array(UInt8)) ENGINE = Memory;"
$ clickhouse-client --query="INSERT INTO msgpack VALUES ([0, 1, 2, 3, 42, 253, 254, 255]), ([255, 254, 253, 42, 3, 2, 1, 0])";
$ clickhouse-client --query="SELECT * FROM msgpack FORMAT MsgPack" > tmp_msgpack.msgpk;
```

<div id="format-settings">
  ## フォーマット設定
</div>

| 設定                                                                                                                                 | 説明                                          | デフォルト |
| ---------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- | ----- |
| [`input_format_msgpack_number_of_columns`](/ja/operations/settings/settings-formats.md/#input_format_msgpack_number_of_columns)       | 挿入する MsgPack データ内のカラム数。データからのスキーマ推論に使用されます。 | `0`   |
| [`output_format_msgpack_uuid_representation`](/ja/operations/settings/settings-formats.md/#output_format_msgpack_uuid_representation) | MsgPack フォーマットで UUID を出力する際の形式。             | `EXT` |