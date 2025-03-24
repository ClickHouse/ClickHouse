---
alias: []
description: 'Documentation for the ORC format'
input_format: true
keywords: ['ORC']
output_format: true
slug: /interfaces/formats/ORC
title: 'ORC'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Apache ORC](https://orc.apache.org/) is a columnar storage format widely used in the [Hadoop](https://hadoop.apache.org/) ecosystem.

## Data Types Matching {#data-types-matching-orc}

The table below compares supported ORC data types and their corresponding ClickHouse [data types](/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| ORC data type (`INSERT`)              | ClickHouse data type                                                                                              | ORC data type (`SELECT`) |
|---------------------------------------|-------------------------------------------------------------------------------------------------------------------|--------------------------|
| `Boolean`                             | [UInt8](/sql-reference/data-types/int-uint.md)                                                            | `Boolean`                |
| `Tinyint`                             | [Int8/UInt8](/sql-reference/data-types/int-uint.md)/[Enum8](/sql-reference/data-types/enum.md)    | `Tinyint`                |
| `Smallint`                            | [Int16/UInt16](/sql-reference/data-types/int-uint.md)/[Enum16](/sql-reference/data-types/enum.md) | `Smallint`               |
| `Int`                                 | [Int32/UInt32](/sql-reference/data-types/int-uint.md)                                                     | `Int`                    |
| `Bigint`                              | [Int64/UInt32](/sql-reference/data-types/int-uint.md)                                                     | `Bigint`                 |
| `Float`                               | [Float32](/sql-reference/data-types/float.md)                                                             | `Float`                  |
| `Double`                              | [Float64](/sql-reference/data-types/float.md)                                                             | `Double`                 |
| `Decimal`                             | [Decimal](/sql-reference/data-types/decimal.md)                                                           | `Decimal`                |
| `Date`                                | [Date32](/sql-reference/data-types/date32.md)                                                             | `Date`                   |
| `Timestamp`                           | [DateTime64](/sql-reference/data-types/datetime64.md)                                                     | `Timestamp`              |
| `String`, `Char`, `Varchar`, `Binary` | [String](/sql-reference/data-types/string.md)                                                             | `Binary`                 |
| `List`                                | [Array](/sql-reference/data-types/array.md)                                                               | `List`                   |
| `Struct`                              | [Tuple](/sql-reference/data-types/tuple.md)                                                               | `Struct`                 |
| `Map`                                 | [Map](/sql-reference/data-types/map.md)                                                                   | `Map`                    |
| `Int`                                 | [IPv4](/sql-reference/data-types/int-uint.md)                                                             | `Int`                    |
| `Binary`                              | [IPv6](/sql-reference/data-types/ipv6.md)                                                                 | `Binary`                 |
| `Binary`                              | [Int128/UInt128/Int256/UInt256](/sql-reference/data-types/int-uint.md)                                    | `Binary`                 |
| `Binary`                              | [Decimal256](/sql-reference/data-types/decimal.md)                                                        | `Binary`                 |

- Other types are not supported.
- Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types also can be nested.
- The data types of ClickHouse table columns do not have to match the corresponding ORC data fields. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/sql-reference/functions/type-conversion-functions#cast) the data to the data type set for the ClickHouse table column.

## Example Usage {#example-usage}

### Inserting Data {#inserting-data-orc}

You can insert ORC data from a file into ClickHouse table using the following command:

```bash
$ cat filename.orc | clickhouse-client --query="INSERT INTO some_table FORMAT ORC"
```

### Selecting Data {#selecting-data-orc}

You can select data from a ClickHouse table and save them into some file in the ORC format using the following command:

```bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT ORC" > {filename.orc}
```

## Format Settings {#format-settings}

| Setting                                                                                                                                                                                                      | Description                                                                            | Default |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|---------|
| [`output_format_arrow_string_as_string`](/operations/settings/settings-formats.md/#output_format_arrow_string_as_string)                                                                             | Use Arrow String type instead of Binary for String columns.                            | `false` |
| [`output_format_orc_compression_method`](/operations/settings/settings-formats.md/#output_format_orc_compression_method)                                                                             | Compression method used in output ORC format. Default value                            | `none`  |
| [`input_format_arrow_case_insensitive_column_matching`](/operations/settings/settings-formats.md/#input_format_arrow_case_insensitive_column_matching)                                               | Ignore case when matching Arrow columns with ClickHouse columns.                       | `false` |
| [`input_format_arrow_allow_missing_columns`](/operations/settings/settings-formats.md/#input_format_arrow_allow_missing_columns)                                                                     | Allow missing columns while reading Arrow data.                                        | `false` |
| [`input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`](/operations/settings/settings-formats.md/#input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference) | Allow skipping columns with unsupported types while schema inference for Arrow format. | `false` |

To exchange data with Hadoop, you can use [HDFS table engine](/engines/table-engines/integrations/hdfs.md).



