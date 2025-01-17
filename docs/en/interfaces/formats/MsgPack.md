---
title : MsgPack
slug : /en/interfaces/formats/MsgPack
keywords : [MsgPack]
---

## Description

ClickHouse supports reading and writing [MessagePack](https://msgpack.org/) data files.

## Data Types Matching

| MessagePack data type (`INSERT`)                                   | ClickHouse data type                                                                                    | MessagePack data type (`SELECT`) |
|--------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|----------------------------------|
| `uint N`, `positive fixint`                                        | [UIntN](/docs/en/sql-reference/data-types/int-uint.md)                                                  | `uint N`                         |
| `int N`, `negative fixint`                                         | [IntN](/docs/en/sql-reference/data-types/int-uint.md)                                                   | `int N`                          |
| `bool`                                                             | [UInt8](/docs/en/sql-reference/data-types/int-uint.md)                                                  | `uint 8`                         |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [String](/docs/en/sql-reference/data-types/string.md)                                                   | `bin 8`, `bin 16`, `bin 32`      |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [FixedString](/docs/en/sql-reference/data-types/fixedstring.md)                                         | `bin 8`, `bin 16`, `bin 32`      |
| `float 32`                                                         | [Float32](/docs/en/sql-reference/data-types/float.md)                                                   | `float 32`                       |
| `float 64`                                                         | [Float64](/docs/en/sql-reference/data-types/float.md)                                                   | `float 64`                       |
| `uint 16`                                                          | [Date](/docs/en/sql-reference/data-types/date.md)                                                       | `uint 16`                        |
| `int 32`                                                           | [Date32](/docs/en/sql-reference/data-types/date32.md)                                                   | `int 32`                         |
| `uint 32`                                                          | [DateTime](/docs/en/sql-reference/data-types/datetime.md)                                               | `uint 32`                        |
| `uint 64`                                                          | [DateTime64](/docs/en/sql-reference/data-types/datetime.md)                                             | `uint 64`                        |
| `fixarray`, `array 16`, `array 32`                                 | [Array](/docs/en/sql-reference/data-types/array.md)/[Tuple](/docs/en/sql-reference/data-types/tuple.md) | `fixarray`, `array 16`, `array 32` |
| `fixmap`, `map 16`, `map 32`                                       | [Map](/docs/en/sql-reference/data-types/map.md)                                                         | `fixmap`, `map 16`, `map 32`     |
| `uint 32`                                                          | [IPv4](/docs/en/sql-reference/data-types/ipv4.md)                                                       | `uint 32`                        |
| `bin 8`                                                            | [String](/docs/en/sql-reference/data-types/string.md)                                                   | `bin 8`                          |
| `int 8`                                                            | [Enum8](/docs/en/sql-reference/data-types/enum.md)                                                      | `int 8`                          |
| `bin 8`                                                            | [(U)Int128/(U)Int256](/docs/en/sql-reference/data-types/int-uint.md)                                    | `bin 8`                          |
| `int 32`                                                           | [Decimal32](/docs/en/sql-reference/data-types/decimal.md)                                               | `int 32`                         |
| `int 64`                                                           | [Decimal64](/docs/en/sql-reference/data-types/decimal.md)                                               | `int 64`                         |
| `bin 8`                                                            | [Decimal128/Decimal256](/docs/en/sql-reference/data-types/decimal.md)                                   | `bin 8 `                         |

## Example Usage

Example:

Writing to a file ".msgpk":

```sql
$ clickhouse-client --query="CREATE TABLE msgpack (array Array(UInt8)) ENGINE = Memory;"
$ clickhouse-client --query="INSERT INTO msgpack VALUES ([0, 1, 2, 3, 42, 253, 254, 255]), ([255, 254, 253, 42, 3, 2, 1, 0])";
$ clickhouse-client --query="SELECT * FROM msgpack FORMAT MsgPack" > tmp_msgpack.msgpk;
```

## Format Settings

- [input_format_msgpack_number_of_columns](/docs/en/operations/settings/settings-formats.md/#input_format_msgpack_number_of_columns) - the number of columns in inserted MsgPack data. Used for automatic schema inference from data. Default value - `0`.
- [output_format_msgpack_uuid_representation](/docs/en/operations/settings/settings-formats.md/#output_format_msgpack_uuid_representation) - the way how to output UUID in MsgPack format. Default value - `EXT`.