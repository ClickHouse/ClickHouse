---
title : BSONEachRow
slug : /en/interfaces/formats/BSONEachRow
keywords : [BSONEachRow]
---

## Description

In this format, ClickHouse formats/parses data as a sequence of BSON documents without any separator between them.
Each row is formatted as a single document and each column is formatted as a single BSON document field with column name as a key.

## Data Types Matching

For output it uses the following correspondence between ClickHouse types and BSON types:

| ClickHouse type                                                                                                       | BSON Type                                                                                                     |
|-----------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| [Bool](/docs/en/sql-reference/data-types/boolean.md)                                                                  | `\x08` boolean                                                                                                |
| [Int8/UInt8](/docs/en/sql-reference/data-types/int-uint.md)/[Enum8](/docs/en/sql-reference/data-types/enum.md)        | `\x10` int32                                                                                                  |
| [Int16/UInt16](/docs/en/sql-reference/data-types/int-uint.md)/[Enum16](/docs/en/sql-reference/data-types/enum.md)      | `\x10` int32                                                                                                  |
| [Int32](/docs/en/sql-reference/data-types/int-uint.md)                                                                | `\x10` int32                                                                                                  |
| [UInt32](/docs/en/sql-reference/data-types/int-uint.md)                                                               | `\x12` int64                                                                                                  |
| [Int64/UInt64](/docs/en/sql-reference/data-types/int-uint.md)                                                         | `\x12` int64                                                                                                  |
| [Float32/Float64](/docs/en/sql-reference/data-types/float.md)                                                         | `\x01` double                                                                                                 |
| [Date](/docs/en/sql-reference/data-types/date.md)/[Date32](/docs/en/sql-reference/data-types/date32.md)               | `\x10` int32                                                                                                  |
| [DateTime](/docs/en/sql-reference/data-types/datetime.md)                                                             | `\x12` int64                                                                                                  |
| [DateTime64](/docs/en/sql-reference/data-types/datetime64.md)                                                         | `\x09` datetime                                                                                               |
| [Decimal32](/docs/en/sql-reference/data-types/decimal.md)                                                             | `\x10` int32                                                                                                  |
| [Decimal64](/docs/en/sql-reference/data-types/decimal.md)                                                             | `\x12` int64                                                                                                  |
| [Decimal128](/docs/en/sql-reference/data-types/decimal.md)                                                            | `\x05` binary, `\x00` binary subtype, size = 16                                                               |
| [Decimal256](/docs/en/sql-reference/data-types/decimal.md)                                                            | `\x05` binary, `\x00` binary subtype, size = 32                                                               |
| [Int128/UInt128](/docs/en/sql-reference/data-types/int-uint.md)                                                       | `\x05` binary, `\x00` binary subtype, size = 16                                                               |
| [Int256/UInt256](/docs/en/sql-reference/data-types/int-uint.md)                                                       | `\x05` binary, `\x00` binary subtype, size = 32                                                               |
| [String](/docs/en/sql-reference/data-types/string.md)/[FixedString](/docs/en/sql-reference/data-types/fixedstring.md) | `\x05` binary, `\x00` binary subtype or \x02 string if setting output_format_bson_string_as_string is enabled |
| [UUID](/docs/en/sql-reference/data-types/uuid.md)                                                                     | `\x05` binary, `\x04` uuid subtype, size = 16                                                                 |
| [Array](/docs/en/sql-reference/data-types/array.md)                                                                   | `\x04` array                                                                                                  |
| [Tuple](/docs/en/sql-reference/data-types/tuple.md)                                                                   | `\x04` array                                                                                                  |
| [Named Tuple](/docs/en/sql-reference/data-types/tuple.md)                                                             | `\x03` document                                                                                               |
| [Map](/docs/en/sql-reference/data-types/map.md)                                                                       | `\x03` document                                                                                               |
| [IPv4](/docs/en/sql-reference/data-types/ipv4.md)                                                                     | `\x10` int32                                                                                                  |
| [IPv6](/docs/en/sql-reference/data-types/ipv6.md)                                                                     | `\x05` binary, `\x00` binary subtype                                                                          |

For input it uses the following correspondence between BSON types and ClickHouse types:

| BSON Type                                | ClickHouse Type                                                                                                                                                                                                                             |
|------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `\x01` double                            | [Float32/Float64](/docs/en/sql-reference/data-types/float.md)                                                                                                                                                                               |
| `\x02` string                            | [String](/docs/en/sql-reference/data-types/string.md)/[FixedString](/docs/en/sql-reference/data-types/fixedstring.md)                                                                                                                       |
| `\x03` document                          | [Map](/docs/en/sql-reference/data-types/map.md)/[Named Tuple](/docs/en/sql-reference/data-types/tuple.md)                                                                                                                                   |
| `\x04` array                             | [Array](/docs/en/sql-reference/data-types/array.md)/[Tuple](/docs/en/sql-reference/data-types/tuple.md)                                                                                                                                     |
| `\x05` binary, `\x00` binary subtype     | [String](/docs/en/sql-reference/data-types/string.md)/[FixedString](/docs/en/sql-reference/data-types/fixedstring.md)/[IPv6](/docs/en/sql-reference/data-types/ipv6.md)                                                             |
| `\x05` binary, `\x02` old binary subtype | [String](/docs/en/sql-reference/data-types/string.md)/[FixedString](/docs/en/sql-reference/data-types/fixedstring.md)                                                                                                                       |
| `\x05` binary, `\x03` old uuid subtype   | [UUID](/docs/en/sql-reference/data-types/uuid.md)                                                                                                                                                                                           |
| `\x05` binary, `\x04` uuid subtype       | [UUID](/docs/en/sql-reference/data-types/uuid.md)                                                                                                                                                                                           |
| `\x07` ObjectId                          | [String](/docs/en/sql-reference/data-types/string.md)/[FixedString](/docs/en/sql-reference/data-types/fixedstring.md)                                                                                                                       |
| `\x08` boolean                           | [Bool](/docs/en/sql-reference/data-types/boolean.md)                                                                                                                                                                                        |
| `\x09` datetime                          | [DateTime64](/docs/en/sql-reference/data-types/datetime64.md)                                                                                                                                                                               |
| `\x0A` null value                        | [NULL](/docs/en/sql-reference/data-types/nullable.md)                                                                                                                                                                                       |
| `\x0D` JavaScript code                   | [String](/docs/en/sql-reference/data-types/string.md)/[FixedString](/docs/en/sql-reference/data-types/fixedstring.md)                                                                                                                       |
| `\x0E` symbol                            | [String](/docs/en/sql-reference/data-types/string.md)/[FixedString](/docs/en/sql-reference/data-types/fixedstring.md)                                                                                                                       |
| `\x10` int32                             | [Int32/UInt32](/docs/en/sql-reference/data-types/int-uint.md)/[Decimal32](/docs/en/sql-reference/data-types/decimal.md)/[IPv4](/docs/en/sql-reference/data-types/ipv4.md)/[Enum8/Enum16](/docs/en/sql-reference/data-types/enum.md) |
| `\x12` int64                             | [Int64/UInt64](/docs/en/sql-reference/data-types/int-uint.md)/[Decimal64](/docs/en/sql-reference/data-types/decimal.md)/[DateTime64](/docs/en/sql-reference/data-types/datetime64.md)                                                       |

Other BSON types are not supported. Also, it performs conversion between different integer types (for example, you can insert BSON int32 value into ClickHouse UInt8).
Big integers and decimals (Int128/UInt128/Int256/UInt256/Decimal128/Decimal256) can be parsed from BSON Binary value with `\x00` binary subtype. In this case this format will validate that the size of binary data equals the size of expected value.

:::note
This format does not work properly on Big-Endian platforms.
:::

## Example Usage

## Format Settings

- [output_format_bson_string_as_string](/docs/en/operations/settings/settings-formats.md/#output_format_bson_string_as_string) - use BSON String type instead of Binary for String columns. Default value - `false`.
- [input_format_bson_skip_fields_with_unsupported_types_in_schema_inference](/docs/en/operations/settings/settings-formats.md/#input_format_bson_skip_fields_with_unsupported_types_in_schema_inference) - allow skipping columns with unsupported types while schema inference for format BSONEachRow. Default value - `false`.