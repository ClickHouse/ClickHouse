---
alias: []
description: 'MsgPack 格式文档'
input_format: true
keywords: ['MsgPack']
output_format: true
slug: /interfaces/formats/MsgPack
title: 'MsgPack'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 说明
</div>

ClickHouse 支持读写 [MessagePack](https://msgpack.org/) 数据文件。

<div id="data-types-matching">
  ## 数据类型匹配
</div>

| MessagePack 数据类型 (`INSERT`)                                        | ClickHouse 数据类型                                                                             | MessagePack 数据类型 (`SELECT`)        |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------- | ---------------------------------- |
| `uint N`, `positive fixint`                                        | [`UIntN`](/zh/sql-reference/data-types/int-uint.md)                                            | `uint N`                           |
| `int N`, `negative fixint`                                         | [`IntN`](/zh/sql-reference/data-types/int-uint.md)                                             | `int N`                            |
| `bool`                                                             | [`UInt8`](/zh/sql-reference/data-types/int-uint.md)                                            | `uint 8`                           |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`String`](/zh/sql-reference/data-types/string.md)                                             | `bin 8`, `bin 16`, `bin 32`        |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [`FixedString`](/zh/sql-reference/data-types/fixedstring.md)                                   | `bin 8`, `bin 16`, `bin 32`        |
| `float 32`                                                         | [`Float32`](/zh/sql-reference/data-types/float.md)                                             | `float 32`                         |
| `float 64`                                                         | [`Float64`](/zh/sql-reference/data-types/float.md)                                             | `float 64`                         |
| `uint 16`                                                          | [`Date`](/zh/sql-reference/data-types/date.md)                                                 | `uint 16`                          |
| `int 32`                                                           | [`Date32`](/zh/sql-reference/data-types/date32.md)                                             | `int 32`                           |
| `uint 32`                                                          | [`DateTime`](/zh/sql-reference/data-types/datetime.md)                                         | `uint 32`                          |
| `uint 64`                                                          | [`DateTime64`](/zh/sql-reference/data-types/datetime.md)                                       | `uint 64`                          |
| `fixarray`, `array 16`, `array 32`                                 | [`Array`](/zh/sql-reference/data-types/array.md)/[`Tuple`](/zh/sql-reference/data-types/tuple.md) | `fixarray`, `array 16`, `array 32` |
| `fixmap`, `map 16`, `map 32`                                       | [`Map`](/zh/sql-reference/data-types/map.md)                                                   | `fixmap`, `map 16`, `map 32`       |
| `uint 32`                                                          | [`IPv4`](/zh/sql-reference/data-types/ipv4.md)                                                 | `uint 32`                          |
| `bin 8`                                                            | [`String`](/zh/sql-reference/data-types/string.md)                                             | `bin 8`                            |
| `int 8`                                                            | [`Enum8`](/zh/sql-reference/data-types/enum.md)                                                | `int 8`                            |
| `bin 8`                                                            | [`(U)Int128`/`(U)Int256`](/zh/sql-reference/data-types/int-uint.md)                            | `bin 8`                            |
| `int 32`                                                           | [`Decimal32`](/zh/sql-reference/data-types/decimal.md)                                         | `int 32`                           |
| `int 64`                                                           | [`Decimal64`](/zh/sql-reference/data-types/decimal.md)                                         | `int 64`                           |
| `bin 8`                                                            | [`Decimal128`/`Decimal256`](/zh/sql-reference/data-types/decimal.md)                           | `bin 8 `                           |

<div id="example-usage">
  ## 示例用法
</div>

写入文件 &quot;.msgpk&quot;：

```sql
$ clickhouse-client --query="CREATE TABLE msgpack (array Array(UInt8)) ENGINE = Memory;"
$ clickhouse-client --query="INSERT INTO msgpack VALUES ([0, 1, 2, 3, 42, 253, 254, 255]), ([255, 254, 253, 42, 3, 2, 1, 0])";
$ clickhouse-client --query="SELECT * FROM msgpack FORMAT MsgPack" > tmp_msgpack.msgpk;
```

<div id="format-settings">
  ## 格式设置
</div>

| 设置项                                                                                                                                | 描述                                    | 默认值   |
| ---------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------- | ----- |
| [`input_format_msgpack_number_of_columns`](/zh/operations/settings/settings-formats.md/#input_format_msgpack_number_of_columns)       | 插入的 MsgPack 数据中的列数。用于根据数据自动推断 schema。 | `0`   |
| [`output_format_msgpack_uuid_representation`](/zh/operations/settings/settings-formats.md/#output_format_msgpack_uuid_representation) | 以 MsgPack 格式输出 UUID 的方式。              | `EXT` |