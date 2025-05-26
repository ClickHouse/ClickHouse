---
alias: []
description: 'Documentation for the Parquet format'
input_format: true
keywords: ['Parquet']
output_format: true
slug: /interfaces/formats/Parquet
title: 'Parquet'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Apache Parquet](https://parquet.apache.org/) is a columnar storage format widespread in the Hadoop ecosystem. ClickHouse supports read and write operations for this format.

## Data Types Matching {#data-types-matching-parquet}

The table below shows supported data types and how they match ClickHouse [data types](/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| Parquet data type (`INSERT`)                  | ClickHouse data type                                                                                       | Parquet data type (`SELECT`)  |
|-----------------------------------------------|------------------------------------------------------------------------------------------------------------|-------------------------------|
| `BOOL`                                        | [Bool](/sql-reference/data-types/boolean.md)                                                       | `BOOL`                        |
| `UINT8`, `BOOL`                               | [UInt8](/sql-reference/data-types/int-uint.md)                                                     | `UINT8`                       |
| `INT8`                                        | [Int8](/sql-reference/data-types/int-uint.md)/[Enum8](/sql-reference/data-types/enum.md)   | `INT8`                        |
| `UINT16`                                      | [UInt16](/sql-reference/data-types/int-uint.md)                                                    | `UINT16`                      |
| `INT16`                                       | [Int16](/sql-reference/data-types/int-uint.md)/[Enum16](/sql-reference/data-types/enum.md) | `INT16`                       |
| `UINT32`                                      | [UInt32](/sql-reference/data-types/int-uint.md)                                                    | `UINT32`                      |
| `INT32`                                       | [Int32](/sql-reference/data-types/int-uint.md)                                                     | `INT32`                       |
| `UINT64`                                      | [UInt64](/sql-reference/data-types/int-uint.md)                                                    | `UINT64`                      |
| `INT64`                                       | [Int64](/sql-reference/data-types/int-uint.md)                                                     | `INT64`                       |
| `FLOAT`                                       | [Float32](/sql-reference/data-types/float.md)                                                      | `FLOAT`                       |
| `DOUBLE`                                      | [Float64](/sql-reference/data-types/float.md)                                                      | `DOUBLE`                      |
| `DATE`                                        | [Date32](/sql-reference/data-types/date.md)                                                        | `DATE`                        |
| `TIME (ms)`                                   | [DateTime](/sql-reference/data-types/datetime.md)                                                  | `UINT32`                      |
| `TIMESTAMP`, `TIME (us, ns)`                  | [DateTime64](/sql-reference/data-types/datetime64.md)                                              | `TIMESTAMP`                   |
| `STRING`, `BINARY`                            | [String](/sql-reference/data-types/string.md)                                                      | `BINARY`                      |
| `STRING`, `BINARY`, `FIXED_LENGTH_BYTE_ARRAY` | [FixedString](/sql-reference/data-types/fixedstring.md)                                            | `FIXED_LENGTH_BYTE_ARRAY`     |
| `DECIMAL`                                     | [Decimal](/sql-reference/data-types/decimal.md)                                                    | `DECIMAL`                     |
| `LIST`                                        | [Array](/sql-reference/data-types/array.md)                                                        | `LIST`                        |
| `STRUCT`                                      | [Tuple](/sql-reference/data-types/tuple.md)                                                        | `STRUCT`                      |
| `MAP`                                         | [Map](/sql-reference/data-types/map.md)                                                            | `MAP`                         |
| `UINT32`                                      | [IPv4](/sql-reference/data-types/ipv4.md)                                                          | `UINT32`                      |
| `FIXED_LENGTH_BYTE_ARRAY`, `BINARY`           | [IPv6](/sql-reference/data-types/ipv6.md)                                                          | `FIXED_LENGTH_BYTE_ARRAY`     |
| `FIXED_LENGTH_BYTE_ARRAY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/sql-reference/data-types/int-uint.md)                             | `FIXED_LENGTH_BYTE_ARRAY`     |

Arrays can be nested and can have a value of `Nullable` type as an argument. `Tuple` and `Map` types can also be nested.

Unsupported Parquet data types are: 
- `FIXED_SIZE_BINARY`
- `JSON`
- `UUID`
- `ENUM`.

Data types of ClickHouse table columns can differ from the corresponding fields of the Parquet data inserted. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/sql-reference/functions/type-conversion-functions#cast) the data to that data type which is set for the ClickHouse table column.

## Example Usage {#example-usage}

### Inserting and Selecting Data {#inserting-and-selecting-data-parquet}

You can insert Parquet data from a file into ClickHouse table using the following command:

```bash
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT Parquet"
```

You can select data from a ClickHouse table and save it into some file in the Parquet format using the following command:

```bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Parquet" > {some_file.pq}
```

To exchange data with Hadoop, you can use the [`HDFS table engine`](/engines/table-engines/integrations/hdfs.md).

## Format Settings {#format-settings}

| Setting                                                                        | Description                                                                                                                                                                                                                       | Default     |
|--------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `input_format_parquet_case_insensitive_column_matching`                        | Ignore case when matching Parquet columns with CH columns.                                                                                                                                                                          | `0`         |
| `input_format_parquet_preserve_order`                                          | Avoid reordering rows when reading from Parquet files. Usually makes it much slower.                                                                                                                                              | `0`         |
| `input_format_parquet_filter_push_down`                                        | When reading Parquet files, skip whole row groups based on the WHERE/PREWHERE expressions and min/max statistics in the Parquet metadata.                                                                                          | `1`         |
| `input_format_parquet_bloom_filter_push_down`                                  | When reading Parquet files, skip whole row groups based on the WHERE expressions and bloom filter in the Parquet metadata.                                                                                                          | `0`         |
| `input_format_parquet_use_native_reader`                                       | When reading Parquet files, to use native reader instead of arrow reader.                                                                                                                                                          | `0`         |
| `input_format_parquet_allow_missing_columns`                                   | Allow missing columns while reading Parquet input formats                                                                                                                                                                          | `1`         |
| `input_format_parquet_local_file_min_bytes_for_seek`                           | Min bytes required for local read (file) to do seek, instead of read with ignore in Parquet input format                                                                                                                          | `8192`      |
| `input_format_parquet_enable_row_group_prefetch`                               | Enable row group prefetching during parquet parsing. Currently, only single-threaded parsing can prefetch.                                                                                                                          | `1`         |
| `input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference` | Skip columns with unsupported types while schema inference for format Parquet                                                                                                                                                      | `0`         |
| `input_format_parquet_max_block_size`                                          | Max block size for parquet reader.                                                                                                                                                                                                | `65409`     |
| `input_format_parquet_prefer_block_bytes`                                      | Average block bytes output by parquet reader                                                                                                                                                                                      | `16744704`  |
| `output_format_parquet_row_group_size`                                         | Target row group size in rows.                                                                                                                                                                                                      | `1000000`   |
| `output_format_parquet_row_group_size_bytes`                                   | Target row group size in bytes, before compression.                                                                                                                                                                                  | `536870912` |
| `output_format_parquet_string_as_string`                                       | Use Parquet String type instead of Binary for String columns.                                                                                                                                                                      | `1`         |
| `output_format_parquet_fixed_string_as_fixed_byte_array`                       | Use Parquet FIXED_LENGTH_BYTE_ARRAY type instead of Binary for FixedString columns.                                                                                                                                                  | `1`         |
| `output_format_parquet_version`                                                | Parquet format version for output format. Supported versions: 1.0, 2.4, 2.6 and 2.latest (default)                                                                                                                                  | `2.latest`  |
| `output_format_parquet_compression_method`                                     | Compression method for Parquet output format. Supported codecs: snappy, lz4, brotli, zstd, gzip, none (uncompressed)                                                                                                              | `zstd`      |
| `output_format_parquet_compliant_nested_types`                                 | In parquet file schema, use name 'element' instead of 'item' for list elements. This is a historical artifact of Arrow library implementation. Generally increases compatibility, except perhaps with some old versions of Arrow. | `1`         | 
| `output_format_parquet_use_custom_encoder`                                     | Use a faster Parquet encoder implementation.                                                                                                                                                                                      | `1`         |
| `output_format_parquet_parallel_encoding`                                      | Do Parquet encoding in multiple threads. Requires output_format_parquet_use_custom_encoder.                                                                                                                                          | `1`         |
| `output_format_parquet_data_page_size`                                         | Target page size in bytes, before compression.                                                                                                                                                                                      | `1048576`   |
| `output_format_parquet_batch_size`                                             | Check page size every this many rows. Consider decreasing if you have columns with average values size above a few KBs.                                                                                                              | `1024`      |
| `output_format_parquet_write_page_index`                                       | Add a possibility to write page index into parquet files.                                                                                                                                                                          | `1`         |
| `input_format_parquet_import_nested`                                           | Obsolete setting, does nothing.                                                                                                                                                                                                   | `0`         |
