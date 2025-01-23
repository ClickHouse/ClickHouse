---
slug: /en/interfaces/formats
sidebar_position: 21
sidebar_label: View all formats...
title: Formats for Input and Output Data
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

ClickHouse can accept and return data in various formats. A format supported for input can be used to parse the data provided to `INSERT`s, to perform `SELECT`s from a file-backed table such as File, URL or HDFS, or to read a dictionary. A format supported for output can be used to arrange the
results of a `SELECT`, and to perform `INSERT`s into a file-backed table.
All format names are case-insensitive.

The supported formats are:

| Format                                                                                    | Input | Output |
|-------------------------------------------------------------------------------------------|------|-------|
| [TabSeparated](#tabseparated)                                                             | ✔    | ✔     |
| [TabSeparatedRaw](#tabseparatedraw)                                                       | ✔    | ✔     |
| [TabSeparatedWithNames](#tabseparatedwithnames)                                           | ✔    | ✔     |
| [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes)                           | ✔    | ✔     |
| [TabSeparatedRawWithNames](#tabseparatedrawwithnames)                                     | ✔    | ✔     |
| [TabSeparatedRawWithNamesAndTypes](#tabseparatedrawwithnamesandtypes)                     | ✔    | ✔     |
| [Template](#format-template)                                                              | ✔    | ✔     |
| [TemplateIgnoreSpaces](#templateignorespaces)                                             | ✔    | ✗     |
| [CSV](#csv)                                                                               | ✔    | ✔     |
| [CSVWithNames](#csvwithnames)                                                             | ✔    | ✔     |
| [CSVWithNamesAndTypes](#csvwithnamesandtypes)                                             | ✔    | ✔     |
| [CustomSeparated](#format-customseparated)                                                | ✔    | ✔     |
| [CustomSeparatedWithNames](#customseparatedwithnames)                                     | ✔    | ✔     |
| [CustomSeparatedWithNamesAndTypes](#customseparatedwithnamesandtypes)                     | ✔    | ✔     |
| [SQLInsert](#sqlinsert)                                                                   | ✗    | ✔     |
| [Values](#data-format-values)                                                             | ✔    | ✔     |
| [Vertical](#vertical)                                                                     | ✗    | ✔     |
| [JSON](#json)                                                                             | ✔    | ✔     |
| [JSONAsString](#jsonasstring)                                                             | ✔    | ✗     |
| [JSONAsObject](#jsonasobject)                                                             | ✔    | ✗     |
| [JSONStrings](#jsonstrings)                                                               | ✔    | ✔     |
| [JSONColumns](#jsoncolumns)                                                               | ✔    | ✔     |
| [JSONColumnsWithMetadata](#jsoncolumnsmonoblock)                                          | ✔    | ✔     |
| [JSONCompact](#jsoncompact)                                                               | ✔    | ✔     |
| [JSONCompactStrings](#jsoncompactstrings)                                                 | ✗    | ✔     |
| [JSONCompactColumns](#jsoncompactcolumns)                                                 | ✔    | ✔     |
| [JSONEachRow](#jsoneachrow)                                                               | ✔    | ✔     |
| [PrettyJSONEachRow](#prettyjsoneachrow)                                                   | ✗    | ✔     |
| [JSONEachRowWithProgress](#jsoneachrowwithprogress)                                       | ✗    | ✔     |
| [JSONStringsEachRow](#jsonstringseachrow)                                                 | ✔    | ✔     |
| [JSONStringsEachRowWithProgress](#jsonstringseachrowwithprogress)                         | ✗    | ✔     |
| [JSONCompactEachRow](#jsoncompacteachrow)                                                 | ✔    | ✔     |
| [JSONCompactEachRowWithNames](#jsoncompacteachrowwithnames)                               | ✔    | ✔     |
| [JSONCompactEachRowWithNamesAndTypes](#jsoncompacteachrowwithnamesandtypes)               | ✔    | ✔     |
| [JSONCompactStringsEachRow](#jsoncompactstringseachrow)                                   | ✔    | ✔     |
| [JSONCompactStringsEachRowWithNames](#jsoncompactstringseachrowwithnames)                 | ✔    | ✔     |
| [JSONCompactStringsEachRowWithNamesAndTypes](#jsoncompactstringseachrowwithnamesandtypes) | ✔    | ✔     |
| [JSONObjectEachRow](#jsonobjecteachrow)                                                   | ✔    | ✔     |
| [BSONEachRow](#bsoneachrow)                                                               | ✔    | ✔     |
| [TSKV](#tskv)                                                                             | ✔    | ✔     |
| [Pretty](#pretty)                                                                         | ✗    | ✔     |
| [PrettyNoEscapes](#prettynoescapes)                                                       | ✗    | ✔     |
| [PrettyMonoBlock](#prettymonoblock)                                                       | ✗    | ✔     |
| [PrettyNoEscapesMonoBlock](#prettynoescapesmonoblock)                                     | ✗    | ✔     |
| [PrettyCompact](#prettycompact)                                                           | ✗    | ✔     |
| [PrettyCompactNoEscapes](#prettycompactnoescapes)                                         | ✗    | ✔     |
| [PrettyCompactMonoBlock](#prettycompactmonoblock)                                         | ✗    | ✔     |
| [PrettyCompactNoEscapesMonoBlock](#prettycompactnoescapesmonoblock)                       | ✗    | ✔     |
| [PrettySpace](#prettyspace)                                                               | ✗    | ✔     |
| [PrettySpaceNoEscapes](#prettyspacenoescapes)                                             | ✗    | ✔     |
| [PrettySpaceMonoBlock](#prettyspacemonoblock)                                             | ✗    | ✔     |
| [PrettySpaceNoEscapesMonoBlock](#prettyspacenoescapesmonoblock)                           | ✗    | ✔     |
| [Prometheus](#prometheus)                                                                 | ✗    | ✔     |
| [Protobuf](#protobuf)                                                                     | ✔    | ✔     |
| [ProtobufSingle](#protobufsingle)                                                         | ✔    | ✔     |
| [ProtobufList](#protobuflist)								                                                     | ✔    | ✔     |
| [Avro](#data-format-avro)                                                                 | ✔    | ✔     |
| [AvroConfluent](#data-format-avro-confluent)                                              | ✔    | ✗     |
| [Parquet](#data-format-parquet)                                                           | ✔    | ✔     |
| [ParquetMetadata](#data-format-parquet-metadata)                                          | ✔    | ✗     |
| [Arrow](#data-format-arrow)                                                               | ✔    | ✔     |
| [ArrowStream](#data-format-arrow-stream)                                                  | ✔    | ✔     |
| [ORC](#data-format-orc)                                                                   | ✔    | ✔     |
| [One](#data-format-one)                                                                   | ✔    | ✗     |
| [Npy](#data-format-npy)                                                                   | ✔    | ✔     |
| [RowBinary](#rowbinary)                                                                   | ✔    | ✔     |
| [RowBinaryWithNames](#rowbinarywithnamesandtypes)                                         | ✔    | ✔     |
| [RowBinaryWithNamesAndTypes](#rowbinarywithnamesandtypes)                                 | ✔    | ✔     |
| [RowBinaryWithDefaults](#rowbinarywithdefaults)                                           | ✔    | ✗     |
| [Native](#native)                                                                         | ✔    | ✔     |
| [Null](#null)                                                                             | ✗    | ✔     |
| [XML](#xml)                                                                               | ✗    | ✔     |
| [CapnProto](#capnproto)                                                                   | ✔    | ✔     |
| [LineAsString](#lineasstring)                                                             | ✔    | ✔     |
| [Regexp](#data-format-regexp)                                                             | ✔    | ✗     |
| [RawBLOB](#rawblob)                                                                       | ✔    | ✔     |
| [MsgPack](#msgpack)                                                                       | ✔    | ✔     |
| [MySQLDump](#mysqldump)                                                                   | ✔    | ✗     |
| [DWARF](#dwarf)                                                                           | ✔    | ✗     |
| [Markdown](#markdown)                                                                     | ✗    | ✔     |
| [Form](#form)                                                                             | ✔    | ✗     |


You can control some format processing parameters with the ClickHouse settings. For more information read the [Settings](/docs/en/operations/settings/settings-formats.md) section.

## TabSeparated {#tabseparated}

See [TabSeparated](../interfaces/formats/TabSeparated/TabSeparated.md)

## TabSeparatedRaw {#tabseparatedraw}

See [TabSeparatedRaw](/en/interfaces/formats/TabSeparatedRaw)

## TabSeparatedWithNames {#tabseparatedwithnames}

See [TabSeparatedWithNames](../interfaces/formats/TabSeparated/TabSeparatedWithNames.md)

## TabSeparatedWithNamesAndTypes {#tabseparatedwithnamesandtypes}

See [TabSeparatedWithNamesAndTypes](../interfaces/formats/TabSeparated/TabSeparatedWithNamesAndTypes.md)

## TabSeparatedRawWithNames {#tabseparatedrawwithnames}

See [TabSeparatedRawWithNames](../interfaces/formats/TabSeparated/TabSeparatedRawWithNames.md)

## TabSeparatedRawWithNamesAndTypes {#tabseparatedrawwithnamesandtypes}

See [TabSeparatedRawWithNamesAndTypes](../interfaces/formats/TabSeparated/TabSeparatedRawWithNamesAndTypes.md)

## Template {#format-template}

See [Template](../interfaces/formats/Template)

## TemplateIgnoreSpaces {#templateignorespaces}

See [TemplateIgnoreSpaces](../interfaces/formats/Template/TemplateIgnoreSpaces.md)

## TSKV {#tskv}

See [TSKV](formats/TabSeparated/TSKV.md)

## CSV {#csv}

See [CSV](../interfaces/formats/CSV/CSV.md)

## CSVWithNames {#csvwithnames}

See [CSVWithNames](formats/CSV/CSVWithNames.md)

## CSVWithNamesAndTypes {#csvwithnamesandtypes}

See [CSVWithNamesAndTypes](formats/CSV/CSVWithNamesAndTypes.md)

## CustomSeparated {#format-customseparated}

See [CustomSeparated](formats/CustomSeparated/CustomSeparated.md)

## CustomSeparatedWithNames {#customseparatedwithnames}

See [CustomSeparatedWithNames](formats/CustomSeparated/CustomSeparatedWithNames.md)

## CustomSeparatedWithNamesAndTypes {#customseparatedwithnamesandtypes}

See [CustomSeparatedWithNamesAndTypes](formats/CustomSeparated/CustomSeparatedWithNamesAndTypes.md)

## SQLInsert {#sqlinsert}

See [SQLInsert](formats/SQLInsert.md)

## JSON {#json}

See [JSON](formats/JSON/JSON.md)

## JSONStrings {#jsonstrings}

See [JSONStrings](formats/JSON/JSONStrings.md)

## JSONColumns {#jsoncolumns}

See [JSONColumns](formats/JSON/JSONColumns.md)

## JSONColumnsWithMetadata {#jsoncolumnsmonoblock}

See [JSONColumnsWithMetadata](formats/JSON/JSONColumnsWithMetadata.md)

## JSONAsString {#jsonasstring}

See [JSONAsString](formats/JSON/JSONAsString.md)

## JSONAsObject {#jsonasobject}

See [JSONAsObject](formats/JSON/JSONAsObject.md)

## JSONCompact {#jsoncompact}

See [JSONCompact](formats/JSON/JSONCompact.md)

## JSONCompactStrings {#jsoncompactstrings}

See [JSONCompactStrings](formats/JSON/JSONCompactStrings.md)

## JSONCompactColumns {#jsoncompactcolumns}

See [JSONCompactColumns](formats/JSON/JSONCompactColumns.md)

## JSONEachRow {#jsoneachrow}

See [JSONEachRow](formats/JSON/JSONEachRow.md)

## PrettyJSONEachRow {#prettyjsoneachrow}

See [PrettyJSONEachRow](formats/JSON/PrettyJSONEachRow.md)

## JSONStringsEachRow {#jsonstringseachrow}

See [JSONStringsEachRow](formats/JSON/JSONStringsEachRow.md)

## JSONCompactEachRow {#jsoncompacteachrow}

See [JSONCompactEachRow](formats/JSON/JSONCompactEachRow.md)

## JSONCompactStringsEachRow {#jsoncompactstringseachrow}

See [JSONCompactStringsEachRow](formats/JSON/JSONCompactStringsEachRow.md)

## JSONEachRowWithProgress {#jsoneachrowwithprogress}

See [JSONEachRowWithProgress](formats/JSON/JSONEachRowWithProgress.md)

## JSONStringsEachRowWithProgress {#jsonstringseachrowwithprogress}

See [JSONStringsEachRowWithProgress](formats/JSON/JSONStringsEachRowWithProgress.md)

## JSONCompactEachRowWithNames {#jsoncompacteachrowwithnames}

See [JSONCompactEachRowWithNames](formats/JSON/JSONCompactEachRowWithNames.md)

## JSONCompactEachRowWithNamesAndTypes {#jsoncompacteachrowwithnamesandtypes}

See [JSONCompactEachRowWithNamesAndTypes](formats/JSON/JSONCompactEachRowWithNamesAndTypes.md)

## JSONCompactStringsEachRowWithNames {#jsoncompactstringseachrowwithnames}

See [JSONCompactStringsEachRowWithNames](formats/JSON/JSONCompactStringsEachRowWithNames.md)

## JSONCompactStringsEachRowWithNamesAndTypes {#jsoncompactstringseachrowwithnamesandtypes}

See [JSONCompactStringsEachRowWithNamesAndTypes](formats/JSON/JSONCompactStringsEachRowWithNamesAndTypes.md)

## JSONObjectEachRow {#jsonobjecteachrow}

See [JSONObjectEachRow](formats/JSON/JSONObjectEachRow.md)

### JSON Formats Settings {#json-formats-settings}

See [JSON Format Settings](formats/JSON/format-settings.md)

## BSONEachRow {#bsoneachrow}

See [BSONEachRow](formats/BSONEachRow.md)

## Native {#native}

See [Native](formats/Native.md)

## Null {#null}

See [Null](formats/Null.md)

## Pretty {#pretty}

See [Pretty](formats/Pretty/Pretty.md)

## PrettyNoEscapes {#prettynoescapes}

See [PrettyNoEscapes](formats/Pretty/PrettyNoEscapes.md)

## PrettyMonoBlock {#prettymonoblock}

See [PrettyMonoBlock](formats/Pretty/PrettyMonoBlock.md)

## PrettyNoEscapesMonoBlock {#prettynoescapesmonoblock}

See [PrettyNoEscapesMonoBlock](formats/Pretty/PrettyNoEscapesMonoBlock.md)

## PrettyCompact {#prettycompact}

See [PrettyCompact](formats/Pretty/PrettyCompact.md)

## PrettyCompactNoEscapes {#prettycompactnoescapes}

See [PrettyCompactNoEscapes](formats/Pretty/PrettyCompactNoEscapes.md)

## PrettyCompactMonoBlock {#prettycompactmonoblock}

See [PrettyCompactMonoBlock](formats/Pretty/PrettyCompactMonoBlock.md)

## PrettyCompactNoEscapesMonoBlock {#prettycompactnoescapesmonoblock}

See [PrettyCompactNoEscapesMonoBlock](formats/Pretty/PrettyCompactNoEscapesMonoBlock.md)

## PrettySpace {#prettyspace}

See [PrettySpace](formats/Pretty/PrettySpace.md)

## PrettySpaceNoEscapes {#prettyspacenoescapes}

See [PrettySpaceNoEscapes](formats/Pretty/PrettySpaceNoEscapes)

## PrettySpaceMonoBlock {#prettyspacemonoblock}

See [PrettySpaceMonoBlock](formats/Pretty/PrettySpaceMonoBlock.md)

## PrettySpaceNoEscapesMonoBlock {#prettyspacenoescapesmonoblock}

See [PrettySpaceNoEscapesMonoBlock](formats/Pretty/PrettySpaceNoEscapesMonoBlock.md)

## RowBinary {#rowbinary}

See [RowBinary](formats/RowBinary/RowBinary.md)

## RowBinaryWithNames {#rowbinarywithnames}

See [RowBinaryWithNames](formats/RowBinary/RowBinaryWithNames.md)

## RowBinaryWithNamesAndTypes {#rowbinarywithnamesandtypes}

See [RowBinaryWithNamesAndTypes](formats/RowBinary/RowBinaryWithNamesAndTypes.md)

## RowBinaryWithDefaults {#rowbinarywithdefaults}

See [RowBinaryWithDefaults](formats/RowBinary/RowBinaryWithDefaults.md)

## Values {#data-format-values}

See [Values](formats/Values.md)

## Vertical {#vertical}

See [Vertical](formats/Vertical.md)

## XML {#xml}

See [XML](formats/XML.md)

## CapnProto {#capnproto}

See [CapnProto](formats/CapnProto.md)

## Prometheus {#prometheus}

See [Prometheus](formats/Prometheus.md)

## Protobuf {#protobuf}

See [Protobuf](formats/Protobuf/Protobuf.md)

## ProtobufSingle {#protobufsingle}

See [ProtobufSingle](formats/Protobuf/ProtobufSingle.md)

## ProtobufList {#protobuflist}

See [ProtobufList](formats/Protobuf/ProtobufList.md)

## Avro {#data-format-avro}

See [Avro](formats/Avro/Avro.md)

## AvroConfluent {#data-format-avro-confluent}

See [AvroConfluent](formats/Avro/AvroConfluent.md)

## Parquet {#data-format-parquet}

[Apache Parquet](https://parquet.apache.org/) is a columnar storage format widespread in the Hadoop ecosystem. ClickHouse supports read and write operations for this format.

### Data Types Matching {#data-types-matching-parquet}

The table below shows supported data types and how they match ClickHouse [data types](/docs/en/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| Parquet data type (`INSERT`)                  | ClickHouse data type                                                                                       | Parquet data type (`SELECT`)  |
|-----------------------------------------------|------------------------------------------------------------------------------------------------------------|-------------------------------|
| `BOOL`                                        | [Bool](/docs/en/sql-reference/data-types/boolean.md)                                                       | `BOOL`                        |
| `UINT8`, `BOOL`                               | [UInt8](/docs/en/sql-reference/data-types/int-uint.md)                                                     | `UINT8`                       |
| `INT8`                                        | [Int8](/docs/en/sql-reference/data-types/int-uint.md)/[Enum8](/docs/en/sql-reference/data-types/enum.md)   | `INT8`                        |
| `UINT16`                                      | [UInt16](/docs/en/sql-reference/data-types/int-uint.md)                                                    | `UINT16`                      |
| `INT16`                                       | [Int16](/docs/en/sql-reference/data-types/int-uint.md)/[Enum16](/docs/en/sql-reference/data-types/enum.md) | `INT16`                       |
| `UINT32`                                      | [UInt32](/docs/en/sql-reference/data-types/int-uint.md)                                                    | `UINT32`                      |
| `INT32`                                       | [Int32](/docs/en/sql-reference/data-types/int-uint.md)                                                     | `INT32`                       |
| `UINT64`                                      | [UInt64](/docs/en/sql-reference/data-types/int-uint.md)                                                    | `UINT64`                      |
| `INT64`                                       | [Int64](/docs/en/sql-reference/data-types/int-uint.md)                                                     | `INT64`                       |
| `FLOAT`                                       | [Float32](/docs/en/sql-reference/data-types/float.md)                                                      | `FLOAT`                       |
| `DOUBLE`                                      | [Float64](/docs/en/sql-reference/data-types/float.md)                                                      | `DOUBLE`                      |
| `DATE`                                        | [Date32](/docs/en/sql-reference/data-types/date.md)                                                        | `DATE`                        |
| `TIME (ms)`                                   | [DateTime](/docs/en/sql-reference/data-types/datetime.md)                                                  | `UINT32`                      |
| `TIMESTAMP`, `TIME (us, ns)`                  | [DateTime64](/docs/en/sql-reference/data-types/datetime64.md)                                              | `TIMESTAMP`                   |
| `STRING`, `BINARY`                            | [String](/docs/en/sql-reference/data-types/string.md)                                                      | `BINARY`                      |
| `STRING`, `BINARY`, `FIXED_LENGTH_BYTE_ARRAY` | [FixedString](/docs/en/sql-reference/data-types/fixedstring.md)                                            | `FIXED_LENGTH_BYTE_ARRAY`     |
| `DECIMAL`                                     | [Decimal](/docs/en/sql-reference/data-types/decimal.md)                                                    | `DECIMAL`                     |
| `LIST`                                        | [Array](/docs/en/sql-reference/data-types/array.md)                                                        | `LIST`                        |
| `STRUCT`                                      | [Tuple](/docs/en/sql-reference/data-types/tuple.md)                                                        | `STRUCT`                      |
| `MAP`                                         | [Map](/docs/en/sql-reference/data-types/map.md)                                                            | `MAP`                         |
| `UINT32`                                      | [IPv4](/docs/en/sql-reference/data-types/ipv4.md)                                                          | `UINT32`                      |
| `FIXED_LENGTH_BYTE_ARRAY`, `BINARY`           | [IPv6](/docs/en/sql-reference/data-types/ipv6.md)                                                          | `FIXED_LENGTH_BYTE_ARRAY`     |
| `FIXED_LENGTH_BYTE_ARRAY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/docs/en/sql-reference/data-types/int-uint.md)                             | `FIXED_LENGTH_BYTE_ARRAY`     |

Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types also can be nested.

Unsupported Parquet data types: `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

Data types of ClickHouse table columns can differ from the corresponding fields of the Parquet data inserted. When inserting data, ClickHouse interprets data types according to the table above and then [cast](/docs/en/sql-reference/functions/type-conversion-functions/#type_conversion_function-cast) the data to that data type which is set for the ClickHouse table column.

### Inserting and Selecting Data {#inserting-and-selecting-data-parquet}

You can insert Parquet data from a file into ClickHouse table by the following command:

``` bash
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT Parquet"
```

You can select data from a ClickHouse table and save them into some file in the Parquet format by the following command:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Parquet" > {some_file.pq}
```

To exchange data with Hadoop, you can use [HDFS table engine](/docs/en/engines/table-engines/integrations/hdfs.md).

### Parquet format settings {#parquet-format-settings}

- [output_format_parquet_row_group_size](/docs/en/operations/settings/settings-formats.md/#output_format_parquet_row_group_size) - row group size in rows while data output. Default value - `1000000`.
- [output_format_parquet_string_as_string](/docs/en/operations/settings/settings-formats.md/#output_format_parquet_string_as_string) - use Parquet String type instead of Binary for String columns. Default value - `false`.
- [input_format_parquet_import_nested](/docs/en/operations/settings/settings-formats.md/#input_format_parquet_import_nested) - allow inserting array of structs into [Nested](/docs/en/sql-reference/data-types/nested-data-structures/index.md) table in Parquet input format. Default value - `false`.
- [input_format_parquet_case_insensitive_column_matching](/docs/en/operations/settings/settings-formats.md/#input_format_parquet_case_insensitive_column_matching) - ignore case when matching Parquet columns with ClickHouse columns. Default value - `false`.
- [input_format_parquet_allow_missing_columns](/docs/en/operations/settings/settings-formats.md/#input_format_parquet_allow_missing_columns) - allow missing columns while reading Parquet data. Default value - `false`.
- [input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference](/docs/en/operations/settings/settings-formats.md/#input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference) - allow skipping columns with unsupported types while schema inference for Parquet format. Default value - `false`.
- [input_format_parquet_local_file_min_bytes_for_seek](/docs/en/operations/settings/settings-formats.md/#input_format_parquet_local_file_min_bytes_for_seek) - min bytes required for local read (file) to do seek, instead of read with ignore in Parquet input format. Default value - `8192`.
- [output_format_parquet_fixed_string_as_fixed_byte_array](/docs/en/operations/settings/settings-formats.md/#output_format_parquet_fixed_string_as_fixed_byte_array) - use Parquet FIXED_LENGTH_BYTE_ARRAY type instead of Binary/String for FixedString columns. Default value - `true`.
- [output_format_parquet_version](/docs/en/operations/settings/settings-formats.md/#output_format_parquet_version) - The version of Parquet format used in output format. Default value - `2.latest`.
- [output_format_parquet_compression_method](/docs/en/operations/settings/settings-formats.md/#output_format_parquet_compression_method) - compression method used in output Parquet format. Default value - `lz4`.
- [input_format_parquet_max_block_size](/docs/en/operations/settings/settings-formats.md/#input_format_parquet_max_block_size) - Max block row size for parquet reader. Default value - `65409`.
- [input_format_parquet_prefer_block_bytes](/docs/en/operations/settings/settings-formats.md/#input_format_parquet_prefer_block_bytes) - Average block bytes output by parquet reader. Default value - `16744704`.
- [output_format_parquet_write_page_index](/docs/en/operations/settings/settings-formats.md/#input_format_parquet_max_block_size) - Add a possibility to write page index into parquet files. Need to disable `output_format_parquet_use_custom_encoder` at present. Default value - `true`.

## ParquetMetadata {#data-format-parquet-metadata}

Special format for reading Parquet file metadata (https://parquet.apache.org/docs/file-format/metadata/). It always outputs one row with the next structure/content:
- num_columns - the number of columns
- num_rows - the total number of rows
- num_row_groups - the total number of row groups
- format_version - parquet format version, always 1.0 or 2.6
- total_uncompressed_size - total uncompressed bytes size of the data, calculated as the sum of total_byte_size from all row groups
- total_compressed_size - total compressed bytes size of the data, calculated as the sum of total_compressed_size from all row groups
- columns - the list of columns metadata with the next structure:
  - name - column name
  - path - column path (differs from name for nested column)
  - max_definition_level - maximum definition level
  - max_repetition_level - maximum repetition level
  - physical_type - column physical type
  - logical_type - column logical type
  - compression - compression used for this column
  - total_uncompressed_size - total uncompressed bytes size of the column, calculated as the sum of total_uncompressed_size of the column from all row groups
  - total_compressed_size - total compressed bytes size of the column,  calculated as the sum of total_compressed_size of the column from all row groups
  - space_saved - percent of space saved by compression, calculated as (1 - total_compressed_size/total_uncompressed_size).
  - encodings - the list of encodings used for this column
- row_groups - the list of row groups metadata with the next structure:
  - num_columns - the number of columns in the row group
  - num_rows - the number of rows in the row group
  - total_uncompressed_size - total uncompressed bytes size of the row group
  - total_compressed_size - total compressed bytes size of the row group
  - columns - the list of column chunks metadata with the next structure:
     - name - column name
     - path - column path
     - total_compressed_size - total compressed bytes size of the column
     - total_uncompressed_size - total uncompressed bytes size of the row group
     - have_statistics - boolean flag that indicates if column chunk metadata contains column statistics
     - statistics - column chunk statistics (all fields are NULL if have_statistics = false) with the next structure:
        - num_values - the number of non-null values in the column chunk
        - null_count - the number of NULL values in the column chunk
        - distinct_count - the number of distinct values in the column chunk
        - min - the minimum value of the column chunk
        - max - the maximum column of the column chunk

Example:

```sql
SELECT * FROM file(data.parquet, ParquetMetadata) format PrettyJSONEachRow
```

```json
{
    "num_columns": "2",
    "num_rows": "100000",
    "num_row_groups": "2",
    "format_version": "2.6",
    "metadata_size": "577",
    "total_uncompressed_size": "282436",
    "total_compressed_size": "26633",
    "columns": [
        {
            "name": "number",
            "path": "number",
            "max_definition_level": "0",
            "max_repetition_level": "0",
            "physical_type": "INT32",
            "logical_type": "Int(bitWidth=16, isSigned=false)",
            "compression": "LZ4",
            "total_uncompressed_size": "133321",
            "total_compressed_size": "13293",
            "space_saved": "90.03%",
            "encodings": [
                "RLE_DICTIONARY",
                "PLAIN",
                "RLE"
            ]
        },
        {
            "name": "concat('Hello', toString(modulo(number, 1000)))",
            "path": "concat('Hello', toString(modulo(number, 1000)))",
            "max_definition_level": "0",
            "max_repetition_level": "0",
            "physical_type": "BYTE_ARRAY",
            "logical_type": "None",
            "compression": "LZ4",
            "total_uncompressed_size": "149115",
            "total_compressed_size": "13340",
            "space_saved": "91.05%",
            "encodings": [
                "RLE_DICTIONARY",
                "PLAIN",
                "RLE"
            ]
        }
    ],
    "row_groups": [
        {
            "num_columns": "2",
            "num_rows": "65409",
            "total_uncompressed_size": "179809",
            "total_compressed_size": "14163",
            "columns": [
                {
                    "name": "number",
                    "path": "number",
                    "total_compressed_size": "7070",
                    "total_uncompressed_size": "85956",
                    "have_statistics": true,
                    "statistics": {
                        "num_values": "65409",
                        "null_count": "0",
                        "distinct_count": null,
                        "min": "0",
                        "max": "999"
                    }
                },
                {
                    "name": "concat('Hello', toString(modulo(number, 1000)))",
                    "path": "concat('Hello', toString(modulo(number, 1000)))",
                    "total_compressed_size": "7093",
                    "total_uncompressed_size": "93853",
                    "have_statistics": true,
                    "statistics": {
                        "num_values": "65409",
                        "null_count": "0",
                        "distinct_count": null,
                        "min": "Hello0",
                        "max": "Hello999"
                    }
                }
            ]
        },
        ...
    ]
}
```

## Arrow {#data-format-arrow}

See [Arrow](formats/Arrow/ArrowStream.md)

## ArrowStream {#data-format-arrow-stream}

See [ArrowStream](formats/Arrow/ArrowStream.md)

## ORC {#data-format-orc}

See [ORC](formats/ORC.md)

## One {#data-format-one}

See [One](formats/One.md)

## Npy {#data-format-npy}

See [Npy](formats/Npy.md)

## LineAsString {#lineasstring}

See [LineAsString](formats/LineAsString/LineAsString.md)

See also: [LineAsStringWithNames](formats/LineAsString/LineAsStringWithNames.md), [LineAsStringWithNamesAndTypes](formats/LineAsString/LineAsStringWithNamesAndTypes.md)

## Regexp {#data-format-regexp}

See [Regexp](formats/Regexp.md)

## Format Schema {#formatschema}

The file name containing the format schema is set by the setting `format_schema`.
It’s required to set this setting when it is used one of the formats `Cap'n Proto` and `Protobuf`.
The format schema is a combination of a file name and the name of a message type in this file, delimited by a colon,
e.g. `schemafile.proto:MessageType`.
If the file has the standard extension for the format (for example, `.proto` for `Protobuf`),
it can be omitted and in this case, the format schema looks like `schemafile:MessageType`.

If you input or output data via the [client](/docs/en/interfaces/cli.md) in interactive mode, the file name specified in the format schema
can contain an absolute path or a path relative to the current directory on the client.
If you use the client in the [batch mode](/docs/en/interfaces/cli.md/#batch-mode), the path to the schema must be relative due to security reasons.

If you input or output data via the [HTTP interface](/docs/en/interfaces/http.md) the file name specified in the format schema
should be located in the directory specified in [format_schema_path](/docs/en/operations/server-configuration-parameters/settings.md/#format_schema_path)
in the server configuration.

## Skipping Errors {#skippingerrors}

Some formats such as `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` and `Protobuf` can skip broken row if parsing error occurred and continue parsing from the beginning of next row. See [input_format_allow_errors_num](/docs/en/operations/settings/settings-formats.md/#input_format_allow_errors_num) and
[input_format_allow_errors_ratio](/docs/en/operations/settings/settings-formats.md/#input_format_allow_errors_ratio) settings.
Limitations:
- In case of parsing error `JSONEachRow` skips all data until the new line (or EOF), so rows must be delimited by `\n` to count errors correctly.
- `Template` and `CustomSeparated` use delimiter after the last column and delimiter between rows to find the beginning of next row, so skipping errors works only if at least one of them is not empty.

## RawBLOB {#rawblob}

See [RawBLOB](formats/RawBLOB.md)

## MsgPack {#msgpack}

See [MsgPack](formats/MsgPack.md)

## MySQLDump {#mysqldump}

See [MySQLDump](formats/MySQLDump.md)

## DWARF {#dwarf}

See [Dwarf](formats/DWARF.md)

## Form {#form}

See [Form](formats/Form.md)