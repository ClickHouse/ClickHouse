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

Outputs data as Unicode-art tables, also using ANSI-escape sequences for setting colours in the terminal.
A full grid of the table is drawn, and each row occupies two lines in the terminal.
Each result block is output as a separate table. This is necessary so that blocks can be output without buffering results (buffering would be necessary in order to pre-calculate the visible width of all the values).

[NULL](/docs/en/sql-reference/syntax.md) is output as `ᴺᵁᴸᴸ`.

Example (shown for the [PrettyCompact](#prettycompact) format):

``` sql
SELECT * FROM t_null
```

``` response
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Rows are not escaped in Pretty\* formats. Example is shown for the [PrettyCompact](#prettycompact) format:

``` sql
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

``` response
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

To avoid dumping too much data to the terminal, only the first 10,000 rows are printed. If the number of rows is greater than or equal to 10,000, the message “Showed first 10 000” is printed.
This format is only appropriate for outputting a query result, but not for parsing (retrieving data to insert in a table).

The Pretty format supports outputting total values (when using WITH TOTALS) and extremes (when ‘extremes’ is set to 1). In these cases, total values and extreme values are output after the main data, in separate tables. Example (shown for the [PrettyCompact](#prettycompact) format):

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT PrettyCompact
```

``` response
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1406958 │
│ 2014-03-18 │ 1383658 │
│ 2014-03-19 │ 1405797 │
│ 2014-03-20 │ 1353623 │
│ 2014-03-21 │ 1245779 │
│ 2014-03-22 │ 1031592 │
│ 2014-03-23 │ 1046491 │
└────────────┴─────────┘

Totals:
┌──EventDate─┬───────c─┐
│ 1970-01-01 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

## PrettyNoEscapes {#prettynoescapes}

Differs from [Pretty](#pretty) in that ANSI-escape sequences aren’t used. This is necessary for displaying this format in a browser, as well as for using the ‘watch’ command-line utility.

Example:

``` bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

You can use the HTTP interface for displaying in the browser.

## PrettyMonoBlock {#prettymonoblock}

Differs from [Pretty](#pretty) in that up to 10,000 rows are buffered, then output as a single table, not by blocks.

## PrettyNoEscapesMonoBlock {#prettynoescapesmonoblock}

Differs from [PrettyNoEscapes](#prettynoescapes) in that up to 10,000 rows are buffered, then output as a single table, not by blocks.


## PrettyCompact {#prettycompact}

Differs from [Pretty](#pretty) in that the grid is drawn between rows and the result is more compact.
This format is used by default in the command-line client in interactive mode.

## PrettyCompactNoEscapes {#prettycompactnoescapes}

Differs from [PrettyCompact](#prettycompact) in that ANSI-escape sequences aren’t used. This is necessary for displaying this format in a browser, as well as for using the ‘watch’ command-line utility.

## PrettyCompactMonoBlock {#prettycompactmonoblock}

Differs from [PrettyCompact](#prettycompact) in that up to 10,000 rows are buffered, then output as a single table, not by blocks.

## PrettyCompactNoEscapesMonoBlock {#prettycompactnoescapesmonoblock}

Differs from [PrettyCompactNoEscapes](#prettycompactnoescapes) in that up to 10,000 rows are buffered, then output as a single table, not by blocks.

## PrettySpace {#prettyspace}

Differs from [PrettyCompact](#prettycompact) in that whitespace (space characters) is used instead of the grid.

## PrettySpaceNoEscapes {#prettyspacenoescapes}

Differs from [PrettySpace](#prettyspace) in that ANSI-escape sequences aren’t used. This is necessary for displaying this format in a browser, as well as for using the ‘watch’ command-line utility.

## PrettySpaceMonoBlock {#prettyspacemonoblock}

Differs from [PrettySpace](#prettyspace) in that up to 10,000 rows are buffered, then output as a single table, not by blocks.

## PrettySpaceNoEscapesMonoBlock {#prettyspacenoescapesmonoblock}

Differs from [PrettySpaceNoEscapes](#prettyspacenoescapes) in that up to 10,000 rows are buffered, then output as a single table, not by blocks.

## Pretty formats settings {#pretty-formats-settings}

- [output_format_pretty_max_rows](/docs/en/operations/settings/settings-formats.md/#output_format_pretty_max_rows) - rows limit for Pretty formats. Default value - `10000`.
- [output_format_pretty_max_column_pad_width](/docs/en/operations/settings/settings-formats.md/#output_format_pretty_max_column_pad_width) - maximum width to pad all values in a column in Pretty formats. Default value - `250`.
- [output_format_pretty_max_value_width](/docs/en/operations/settings/settings-formats.md/#output_format_pretty_max_value_width) - Maximum width of value to display in Pretty formats. If greater - it will be cut. Default value - `10000`.
- [output_format_pretty_color](/docs/en/operations/settings/settings-formats.md/#output_format_pretty_color) - use ANSI escape sequences to paint colors in Pretty formats. Default value - `true`.
- [output_format_pretty_grid_charset](/docs/en/operations/settings/settings-formats.md/#output_format_pretty_grid_charset) - Charset for printing grid borders. Available charsets: ASCII, UTF-8. Default value - `UTF-8`.
- [output_format_pretty_row_numbers](/docs/en/operations/settings/settings-formats.md/#output_format_pretty_row_numbers) - Add row numbers before each row for pretty output format. Default value - `true`.
- [output_format_pretty_display_footer_column_names](/docs/en/operations/settings/settings-formats.md/#output_format_pretty_display_footer_column_names) - Display column names in the footer if table contains many rows. Default value - `true`.
- [output_format_pretty_display_footer_column_names_min_rows](/docs/en/operations/settings/settings-formats.md/#output_format_pretty_display_footer_column_names_min_rows) - Sets the minimum number of rows for which a footer will be displayed if [output_format_pretty_display_footer_column_names](/docs/en/operations/settings/settings-formats.md/#output_format_pretty_display_footer_column_names) is enabled. Default value - 50.

## RowBinary {#rowbinary}

Formats and parses data by row in binary format. Rows and values are listed consecutively, without separators. Because data is in the binary format the delimiter after `FORMAT RowBinary` is strictly specified as next: any number of whitespaces (`' '` - space, code `0x20`; `'\t'` - tab, code `0x09`; `'\f'` - form feed, code `0x0C`) followed by exactly one new line sequence (Windows style `"\r\n"` or Unix style `'\n'`), immediately followed by binary data.
This format is less efficient than the Native format since it is row-based.

Integers use fixed-length little-endian representation. For example, UInt64 uses 8 bytes.
DateTime is represented as UInt32 containing the Unix timestamp as the value.
Date is represented as a UInt16 object that contains the number of days since 1970-01-01 as the value.
String is represented as a varint length (unsigned [LEB128](https://en.wikipedia.org/wiki/LEB128)), followed by the bytes of the string.
FixedString is represented simply as a sequence of bytes.

Array is represented as a varint length (unsigned [LEB128](https://en.wikipedia.org/wiki/LEB128)), followed by successive elements of the array.

For [NULL](/docs/en/sql-reference/syntax.md/#null-literal) support, an additional byte containing 1 or 0 is added before each [Nullable](/docs/en/sql-reference/data-types/nullable.md) value. If 1, then the value is `NULL` and this byte is interpreted as a separate value. If 0, the value after the byte is not `NULL`.

## RowBinaryWithNames {#rowbinarywithnames}

Similar to [RowBinary](#rowbinary), but with added header:

- [LEB128](https://en.wikipedia.org/wiki/LEB128)-encoded number of columns (N)
- N `String`s specifying column names

:::note
If setting [input_format_with_names_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
:::

## RowBinaryWithNamesAndTypes {#rowbinarywithnamesandtypes}

Similar to [RowBinary](#rowbinary), but with added header:

- [LEB128](https://en.wikipedia.org/wiki/LEB128)-encoded number of columns (N)
- N `String`s specifying column names
- N `String`s specifying column types

:::note
If setting [input_format_with_names_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
If setting [input_format_with_types_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to 1,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::

## RowBinaryWithDefaults {#rowbinarywithdefaults}

Similar to [RowBinary](#rowbinary), but with an extra byte before each column that indicates if default value should be used.

Examples:

```sql
:) select * from format('RowBinaryWithDefaults', 'x UInt32 default 42, y UInt32', x'010001000000')

┌──x─┬─y─┐
│ 42 │ 1 │
└────┴───┘
```

For column `x` there is only one byte `01` that indicates that default value should be used and no other data after this byte is provided.
For column `y` data starts with byte `00` that indicates that column has actual value that should be read from the subsequent data `01000000`.

## RowBinary format settings {#row-binary-format-settings}

- [format_binary_max_string_size](/docs/en/operations/settings/settings-formats.md/#format_binary_max_string_size) - The maximum allowed size for String in RowBinary format. Default value - `1GiB`.
- [output_format_binary_encode_types_in_binary_format](/docs/en/operations/settings/settings-formats.md/#output_format_binary_encode_types_in_binary_format) - Allows to write types in header using [binary encoding](/docs/en/sql-reference/data-types/data-types-binary-encoding.md) instead of strings with type names in RowBinaryWithNamesAndTypes output format. Default value - `false`.
- [input_format_binary_encode_types_in_binary_format](/docs/en/operations/settings/settings-formats.md/#input_format_binary_encode_types_in_binary_format) - Allows to read types in header using [binary encoding](/docs/en/sql-reference/data-types/data-types-binary-encoding.md) instead of strings with type names in RowBinaryWithNamesAndTypes input format. Default value - `false`.
- [output_format_binary_write_json_as_string](/docs/en/operations/settings/settings-formats.md/#output_format_binary_write_json_as_string) - Allows to write values of [JSON](/docs/en/sql-reference/data-types/newjson.md) data type as JSON [String](/docs/en/sql-reference/data-types/string.md) values in RowBinary output format. Default value - `false`.
- [input_format_binary_read_json_as_string](/docs/en/operations/settings/settings-formats.md/#input_format_binary_read_json_as_string) - Allows to read values of [JSON](/docs/en/sql-reference/data-types/newjson.md) data type as JSON [String](/docs/en/sql-reference/data-types/string.md) values in RowBinary input format. Default value - `false`.

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

[Apache Avro](https://avro.apache.org/) is a row-oriented data serialization framework developed within Apache’s Hadoop project.

ClickHouse Avro format supports reading and writing [Avro data files](https://avro.apache.org/docs/current/spec.html#Object+Container+Files).

### Data Types Matching {#data_types-matching}

The table below shows supported data types and how they match ClickHouse [data types](/docs/en/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| Avro data type `INSERT`                     | ClickHouse data type                                                                                                          | Avro data type `SELECT`         |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|---------------------------------|
| `boolean`, `int`, `long`, `float`, `double` | [Int(8\16\32)](/docs/en/sql-reference/data-types/int-uint.md), [UInt(8\16\32)](/docs/en/sql-reference/data-types/int-uint.md) | `int`                           |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](/docs/en/sql-reference/data-types/int-uint.md), [UInt64](/docs/en/sql-reference/data-types/int-uint.md)               | `long`                          |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](/docs/en/sql-reference/data-types/float.md)                                                                         | `float`                         |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](/docs/en/sql-reference/data-types/float.md)                                                                         | `double`                        |
| `bytes`, `string`, `fixed`, `enum`          | [String](/docs/en/sql-reference/data-types/string.md)                                                                         | `bytes` or `string` \*          |
| `bytes`, `string`, `fixed`                  | [FixedString(N)](/docs/en/sql-reference/data-types/fixedstring.md)                                                            | `fixed(N)`                      |
| `enum`                                      | [Enum(8\16)](/docs/en/sql-reference/data-types/enum.md)                                                                       | `enum`                          |
| `array(T)`                                  | [Array(T)](/docs/en/sql-reference/data-types/array.md)                                                                        | `array(T)`                      |
| `map(V, K)`                                 | [Map(V, K)](/docs/en/sql-reference/data-types/map.md)                                                                         | `map(string, K)`                |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](/docs/en/sql-reference/data-types/date.md)                                                                      | `union(null, T)`                |
| `union(T1, T2, …)` \**                      | [Variant(T1, T2, …)](/docs/en/sql-reference/data-types/variant.md)                                                            | `union(T1, T2, …)` \**          |
| `null`                                      | [Nullable(Nothing)](/docs/en/sql-reference/data-types/special-data-types/nothing.md)                                          | `null`                          |
| `int (date)` \**\*                          | [Date](/docs/en/sql-reference/data-types/date.md), [Date32](docs/en/sql-reference/data-types/date32.md)                       | `int (date)` \**\*              |
| `long (timestamp-millis)` \**\*             | [DateTime64(3)](/docs/en/sql-reference/data-types/datetime.md)                                                                | `long (timestamp-millis)` \**\* |
| `long (timestamp-micros)` \**\*             | [DateTime64(6)](/docs/en/sql-reference/data-types/datetime.md)                                                                | `long (timestamp-micros)` \**\* |
| `bytes (decimal)`  \**\*                    | [DateTime64(N)](/docs/en/sql-reference/data-types/datetime.md)                                                                | `bytes (decimal)`  \**\*        |
| `int`                                       | [IPv4](/docs/en/sql-reference/data-types/ipv4.md)                                                                             | `int`                           |
| `fixed(16)`                                 | [IPv6](/docs/en/sql-reference/data-types/ipv6.md)                                                                             | `fixed(16)`                     |
| `bytes (decimal)` \**\*                     | [Decimal(P, S)](/docs/en/sql-reference/data-types/decimal.md)                                                                 | `bytes (decimal)` \**\*         |
| `string (uuid)` \**\*                       | [UUID](/docs/en/sql-reference/data-types/uuid.md)                                                                             | `string (uuid)` \**\*           |
| `fixed(16)`                                 | [Int128/UInt128](/docs/en/sql-reference/data-types/int-uint.md)                                                               | `fixed(16)`                     |
| `fixed(32)`                                 | [Int256/UInt256](/docs/en/sql-reference/data-types/int-uint.md)                                                               | `fixed(32)`                     |
| `record`                                    | [Tuple](/docs/en/sql-reference/data-types/tuple.md)                                                                           | `record`                        |



\* `bytes` is default, controlled by [output_format_avro_string_column_pattern](/docs/en/operations/settings/settings-formats.md/#output_format_avro_string_column_pattern)

\**  [Variant type](/docs/en/sql-reference/data-types/variant) implicitly accepts `null` as a field value, so for example the Avro `union(T1, T2, null)` will be converted to `Variant(T1, T2)`.
As a result, when producing Avro from ClickHouse, we have to always include the `null` type to the Avro `union` type set as we don't know if any value is actually `null` during the schema inference.

\**\* [Avro logical types](https://avro.apache.org/docs/current/spec.html#Logical+Types)

Unsupported Avro logical data types: `time-millis`, `time-micros`, `duration`

### Inserting Data {#inserting-data-1}

To insert data from an Avro file into ClickHouse table:

``` bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

The root schema of input Avro file must be of `record` type.

To find the correspondence between table columns and fields of Avro schema ClickHouse compares their names. This comparison is case-sensitive.
Unused fields are skipped.

Data types of ClickHouse table columns can differ from the corresponding fields of the Avro data inserted. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/docs/en/sql-reference/functions/type-conversion-functions.md/#type_conversion_function-cast) the data to corresponding column type.

While importing data, when field is not found in schema and setting [input_format_avro_allow_missing_fields](/docs/en/operations/settings/settings-formats.md/#input_format_avro_allow_missing_fields) is enabled, default value will be used instead of error.

### Selecting Data {#selecting-data-1}

To select data from ClickHouse table into an Avro file:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

Column names must:

- start with `[A-Za-z_]`
- subsequently contain only `[A-Za-z0-9_]`

Output Avro file compression and sync interval can be configured with [output_format_avro_codec](/docs/en/operations/settings/settings-formats.md/#output_format_avro_codec) and [output_format_avro_sync_interval](/docs/en/operations/settings/settings-formats.md/#output_format_avro_sync_interval) respectively.

### Example Data {#example-data-avro}

Using the ClickHouse [DESCRIBE](/docs/en/sql-reference/statements/describe-table) function, you can quickly view the inferred format of an Avro file like the following example. This example includes the URL of a publicly accessible Avro file in the ClickHouse S3 public bucket:

```
DESCRIBE url('https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits.avro','Avro);
```
```
┌─name───────────────────────┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ WatchID                    │ Int64           │              │                    │         │                  │                │
│ JavaEnable                 │ Int32           │              │                    │         │                  │                │
│ Title                      │ String          │              │                    │         │                  │                │
│ GoodEvent                  │ Int32           │              │                    │         │                  │                │
│ EventTime                  │ Int32           │              │                    │         │                  │                │
│ EventDate                  │ Date32          │              │                    │         │                  │                │
│ CounterID                  │ Int32           │              │                    │         │                  │                │
│ ClientIP                   │ Int32           │              │                    │         │                  │                │
│ ClientIP6                  │ FixedString(16) │              │                    │         │                  │                │
│ RegionID                   │ Int32           │              │                    │         │                  │                │
...
│ IslandID                   │ FixedString(16) │              │                    │         │                  │                │
│ RequestNum                 │ Int32           │              │                    │         │                  │                │
│ RequestTry                 │ Int32           │              │                    │         │                  │                │
└────────────────────────────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

## AvroConfluent {#data-format-avro-confluent}

AvroConfluent supports decoding single-object Avro messages commonly used with [Kafka](https://kafka.apache.org/) and [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html).

Each Avro message embeds a schema id that can be resolved to the actual schema with help of the Schema Registry.

Schemas are cached once resolved.

Schema Registry URL is configured with [format_avro_schema_registry_url](/docs/en/operations/settings/settings-formats.md/#format_avro_schema_registry_url).

### Data Types Matching {#data_types-matching-1}

Same as [Avro](#data-format-avro).

### Usage {#usage}

To quickly verify schema resolution you can use [kafkacat](https://github.com/edenhill/kafkacat) with [clickhouse-local](/docs/en/operations/utilities/clickhouse-local.md):

``` bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```

To use `AvroConfluent` with [Kafka](/docs/en/engines/table-engines/integrations/kafka.md):

``` sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent';

-- for debug purposes you can set format_avro_schema_registry_url in a session.
-- this way cannot be used in production
SET format_avro_schema_registry_url = 'http://schema-registry';

SELECT * FROM topic1_stream;
```

:::note
Setting `format_avro_schema_registry_url` needs to be configured in `users.xml` to maintain it’s value after a restart. Also you can use the `format_avro_schema_registry_url` setting of the `Kafka` table engine.
:::

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

[Apache ORC](https://orc.apache.org/) is a columnar storage format widespread in the [Hadoop](https://hadoop.apache.org/) ecosystem.

### Data Types Matching {#data-types-matching-orc}

The table below shows supported data types and how they match ClickHouse [data types](/docs/en/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| ORC data type (`INSERT`)              | ClickHouse data type                                                                                              | ORC data type (`SELECT`) |
|---------------------------------------|-------------------------------------------------------------------------------------------------------------------|--------------------------|
| `Boolean`                             | [UInt8](/docs/en/sql-reference/data-types/int-uint.md)                                                            | `Boolean`                |
| `Tinyint`                             | [Int8/UInt8](/docs/en/sql-reference/data-types/int-uint.md)/[Enum8](/docs/en/sql-reference/data-types/enum.md)    | `Tinyint`                |
| `Smallint`                            | [Int16/UInt16](/docs/en/sql-reference/data-types/int-uint.md)/[Enum16](/docs/en/sql-reference/data-types/enum.md) | `Smallint`               |
| `Int`                                 | [Int32/UInt32](/docs/en/sql-reference/data-types/int-uint.md)                                                     | `Int`                    |
| `Bigint`                              | [Int64/UInt32](/docs/en/sql-reference/data-types/int-uint.md)                                                     | `Bigint`                 |
| `Float`                               | [Float32](/docs/en/sql-reference/data-types/float.md)                                                             | `Float`                  |
| `Double`                              | [Float64](/docs/en/sql-reference/data-types/float.md)                                                             | `Double`                 |
| `Decimal`                             | [Decimal](/docs/en/sql-reference/data-types/decimal.md)                                                           | `Decimal`                |
| `Date`                                | [Date32](/docs/en/sql-reference/data-types/date32.md)                                                             | `Date`                   |
| `Timestamp`                           | [DateTime64](/docs/en/sql-reference/data-types/datetime64.md)                                                     | `Timestamp`              |
| `String`, `Char`, `Varchar`, `Binary` | [String](/docs/en/sql-reference/data-types/string.md)                                                             | `Binary`                 |
| `List`                                | [Array](/docs/en/sql-reference/data-types/array.md)                                                               | `List`                   |
| `Struct`                              | [Tuple](/docs/en/sql-reference/data-types/tuple.md)                                                               | `Struct`                 |
| `Map`                                 | [Map](/docs/en/sql-reference/data-types/map.md)                                                                   | `Map`                    |
| `Int`                                 | [IPv4](/docs/en/sql-reference/data-types/int-uint.md)                                                             | `Int`                    |
| `Binary`                              | [IPv6](/docs/en/sql-reference/data-types/ipv6.md)                                                                 | `Binary`                 |
| `Binary`                              | [Int128/UInt128/Int256/UInt256](/docs/en/sql-reference/data-types/int-uint.md)                                    | `Binary`                 |
| `Binary`                              | [Decimal256](/docs/en/sql-reference/data-types/decimal.md)                                                        | `Binary`                 |

Other types are not supported.

Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types also can be nested.

The data types of ClickHouse table columns do not have to match the corresponding ORC data fields. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/docs/en/sql-reference/functions/type-conversion-functions.md/#type_conversion_function-cast) the data to the data type set for the ClickHouse table column.

### Inserting Data {#inserting-data-orc}

You can insert ORC data from a file into ClickHouse table by the following command:

``` bash
$ cat filename.orc | clickhouse-client --query="INSERT INTO some_table FORMAT ORC"
```

### Selecting Data {#selecting-data-orc}

You can select data from a ClickHouse table and save them into some file in the ORC format by the following command:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT ORC" > {filename.orc}
```

### Arrow format settings {#parquet-format-settings}

- [output_format_arrow_string_as_string](/docs/en/operations/settings/settings-formats.md/#output_format_arrow_string_as_string) - use Arrow String type instead of Binary for String columns. Default value - `false`.
- [output_format_orc_compression_method](/docs/en/operations/settings/settings-formats.md/#output_format_orc_compression_method) - compression method used in output ORC format. Default value - `none`.
- [input_format_arrow_case_insensitive_column_matching](/docs/en/operations/settings/settings-formats.md/#input_format_arrow_case_insensitive_column_matching) - ignore case when matching Arrow columns with ClickHouse columns. Default value - `false`.
- [input_format_arrow_allow_missing_columns](/docs/en/operations/settings/settings-formats.md/#input_format_arrow_allow_missing_columns) - allow missing columns while reading Arrow data. Default value - `false`.
- [input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference](/docs/en/operations/settings/settings-formats.md/#input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference) - allow skipping columns with unsupported types while schema inference for Arrow format. Default value - `false`.


To exchange data with Hadoop, you can use [HDFS table engine](/docs/en/engines/table-engines/integrations/hdfs.md).

## One {#data-format-one}

Special input format that doesn't read any data from file and returns only one row with column of type `UInt8`, name `dummy` and value `0` (like `system.one` table).
Can be used with virtual columns `_file/_path`  to list all files without reading actual data.

Example:

Query:
```sql
SELECT _file FROM file('path/to/files/data*', One);
```

Result:
```text
┌─_file────┐
│ data.csv │
└──────────┘
┌─_file──────┐
│ data.jsonl │
└────────────┘
┌─_file────┐
│ data.tsv │
└──────────┘
┌─_file────────┐
│ data.parquet │
└──────────────┘
```

## Npy {#data-format-npy}

This function is designed to load a NumPy array from a .npy file into ClickHouse. The NumPy file format is a binary format used for efficiently storing arrays of numerical data. During import, ClickHouse treats top level dimension as an array of rows with single column. Supported Npy data types and their corresponding type in ClickHouse:

| Npy data type (`INSERT`) | ClickHouse data type                                            | Npy data type (`SELECT`) |
|--------------------------|-----------------------------------------------------------------|--------------------------|
| `i1`                     | [Int8](/docs/en/sql-reference/data-types/int-uint.md)           | `i1`                     |
| `i2`                     | [Int16](/docs/en/sql-reference/data-types/int-uint.md)          | `i2`                     |
| `i4`                     | [Int32](/docs/en/sql-reference/data-types/int-uint.md)          | `i4`                     |
| `i8`                     | [Int64](/docs/en/sql-reference/data-types/int-uint.md)          | `i8`                     |
| `u1`, `b1`               | [UInt8](/docs/en/sql-reference/data-types/int-uint.md)          | `u1`                     |
| `u2`                     | [UInt16](/docs/en/sql-reference/data-types/int-uint.md)         | `u2`                     |
| `u4`                     | [UInt32](/docs/en/sql-reference/data-types/int-uint.md)         | `u4`                     |
| `u8`                     | [UInt64](/docs/en/sql-reference/data-types/int-uint.md)         | `u8`                     |
| `f2`, `f4`               | [Float32](/docs/en/sql-reference/data-types/float.md)           | `f4`                     |
| `f8`                     | [Float64](/docs/en/sql-reference/data-types/float.md)           | `f8`                     |
| `S`, `U`                 | [String](/docs/en/sql-reference/data-types/string.md)           | `S`                      |
|                          | [FixedString](/docs/en/sql-reference/data-types/fixedstring.md) | `S`                      |

**Example of saving an array in .npy format using Python**


```Python
import numpy as np
arr = np.array([[[1],[2],[3]],[[4],[5],[6]]])
np.save('example_array.npy', arr)
```

**Example of reading a NumPy file in ClickHouse**

Query:
```sql
SELECT *
FROM file('example_array.npy', Npy)
```

Result:
```
┌─array─────────┐
│ [[1],[2],[3]] │
│ [[4],[5],[6]] │
└───────────────┘
```

**Selecting Data**

You can select data from a ClickHouse table and save them into some file in the Npy format by the following command:

```bash
$ clickhouse-client --query="SELECT {column} FROM {some_table} FORMAT Npy" > {filename.npy}
```

## LineAsString {#lineasstring}

In this format, every line of input data is interpreted as a single string value. This format can only be parsed for table with a single field of type [String](/docs/en/sql-reference/data-types/string.md). The remaining columns must be set to [DEFAULT](/docs/en/sql-reference/statements/create/table.md/#default) or [MATERIALIZED](/docs/en/sql-reference/statements/create/table.md/#materialized), or omitted.

**Example**

Query:

``` sql
DROP TABLE IF EXISTS line_as_string;
CREATE TABLE line_as_string (field String) ENGINE = Memory;
INSERT INTO line_as_string FORMAT LineAsString "I love apple", "I love banana", "I love orange";
SELECT * FROM line_as_string;
```

Result:

``` text
┌─field─────────────────────────────────────────────┐
│ "I love apple", "I love banana", "I love orange"; │
└───────────────────────────────────────────────────┘
```

## Regexp {#data-format-regexp}

Each line of imported data is parsed according to the regular expression.

When working with the `Regexp` format, you can use the following settings:

- `format_regexp` — [String](/docs/en/sql-reference/data-types/string.md). Contains regular expression in the [re2](https://github.com/google/re2/wiki/Syntax) format.

- `format_regexp_escaping_rule` — [String](/docs/en/sql-reference/data-types/string.md). The following escaping rules are supported:

    - CSV (similarly to [CSV](#csv))
    - JSON (similarly to [JSONEachRow](#jsoneachrow))
    - Escaped (similarly to [TSV](#tabseparated))
    - Quoted (similarly to [Values](#data-format-values))
    - Raw (extracts subpatterns as a whole, no escaping rules, similarly to [TSVRaw](#tabseparatedraw))

- `format_regexp_skip_unmatched` — [UInt8](/docs/en/sql-reference/data-types/int-uint.md). Defines the need to throw an exception in case the `format_regexp` expression does not match the imported data. Can be set to `0` or `1`.

**Usage**

The regular expression from [format_regexp](/docs/en/operations/settings/settings-formats.md/#format_regexp) setting is applied to every line of imported data. The number of subpatterns in the regular expression must be equal to the number of columns in imported dataset.

Lines of the imported data must be separated by newline character `'\n'` or DOS-style newline `"\r\n"`.

The content of every matched subpattern is parsed with the method of corresponding data type, according to [format_regexp_escaping_rule](/docs/en/operations/settings/settings-formats.md/#format_regexp_escaping_rule) setting.

If the regular expression does not match the line and [format_regexp_skip_unmatched](/docs/en/operations/settings/settings-formats.md/#format_regexp_escaping_rule) is set to 1, the line is silently skipped. Otherwise, exception is thrown.

**Example**

Consider the file data.tsv:

```text
id: 1 array: [1,2,3] string: str1 date: 2020-01-01
id: 2 array: [1,2,3] string: str2 date: 2020-01-02
id: 3 array: [1,2,3] string: str3 date: 2020-01-03
```
and the table:

```sql
CREATE TABLE imp_regex_table (id UInt32, array Array(UInt32), string String, date Date) ENGINE = Memory;
```

Import command:

```bash
$ cat data.tsv | clickhouse-client  --query "INSERT INTO imp_regex_table SETTINGS format_regexp='id: (.+?) array: (.+?) string: (.+?) date: (.+?)', format_regexp_escaping_rule='Escaped', format_regexp_skip_unmatched=0 FORMAT Regexp;"
```

Query:

```sql
SELECT * FROM imp_regex_table;
```

Result:

```text
┌─id─┬─array───┬─string─┬───────date─┐
│  1 │ [1,2,3] │ str1   │ 2020-01-01 │
│  2 │ [1,2,3] │ str2   │ 2020-01-02 │
│  3 │ [1,2,3] │ str3   │ 2020-01-03 │
└────┴─────────┴────────┴────────────┘
```

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

In this format, all input data is read to a single value. It is possible to parse only a table with a single field of type [String](/docs/en/sql-reference/data-types/string.md) or similar.
The result is output in binary format without delimiters and escaping. If more than one value is output, the format is ambiguous, and it will be impossible to read the data back.

Below is a comparison of the formats `RawBLOB` and [TabSeparatedRaw](#tabseparatedraw).

`RawBLOB`:
- data is output in binary format, no escaping;
- there are no delimiters between values;
- no newline at the end of each value.

`TabSeparatedRaw`:
- data is output without escaping;
- the rows contain values separated by tabs;
- there is a line feed after the last value in every row.

The following is a comparison of the `RawBLOB` and [RowBinary](#rowbinary) formats.

`RawBLOB`:
- String fields are output without being prefixed by length.

`RowBinary`:
- String fields are represented as length in varint format (unsigned [LEB128] (https://en.wikipedia.org/wiki/LEB128)), followed by the bytes of the string.

When an empty data is passed to the `RawBLOB` input, ClickHouse throws an exception:

``` text
Code: 108. DB::Exception: No data to insert
```

**Example**

``` bash
$ clickhouse-client --query "CREATE TABLE {some_table} (a String) ENGINE = Memory;"
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT RawBLOB"
$ clickhouse-client --query "SELECT * FROM {some_table} FORMAT RawBLOB" | md5sum
```

Result:

``` text
f9725a22f9191e064120d718e26862a9  -
```

## MsgPack {#msgpack}

ClickHouse supports reading and writing [MessagePack](https://msgpack.org/) data files.

### Data Types Matching {#data-types-matching-msgpack}

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

Example:

Writing to a file ".msgpk":

```sql
$ clickhouse-client --query="CREATE TABLE msgpack (array Array(UInt8)) ENGINE = Memory;"
$ clickhouse-client --query="INSERT INTO msgpack VALUES ([0, 1, 2, 3, 42, 253, 254, 255]), ([255, 254, 253, 42, 3, 2, 1, 0])";
$ clickhouse-client --query="SELECT * FROM msgpack FORMAT MsgPack" > tmp_msgpack.msgpk;
```

### MsgPack format settings {#msgpack-format-settings}

- [input_format_msgpack_number_of_columns](/docs/en/operations/settings/settings-formats.md/#input_format_msgpack_number_of_columns) - the number of columns in inserted MsgPack data. Used for automatic schema inference from data. Default value - `0`.
- [output_format_msgpack_uuid_representation](/docs/en/operations/settings/settings-formats.md/#output_format_msgpack_uuid_representation) - the way how to output UUID in MsgPack format. Default value - `EXT`.

## MySQLDump {#mysqldump}

ClickHouse supports reading MySQL [dumps](https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html).
It reads all data from INSERT queries belonging to one table in dump. If there are more than one table, by default it reads data from the first one.
You can specify the name of the table from which to read data from using [input_format_mysql_dump_table_name](/docs/en/operations/settings/settings-formats.md/#input_format_mysql_dump_table_name) settings.
If setting [input_format_mysql_dump_map_columns](/docs/en/operations/settings/settings-formats.md/#input_format_mysql_dump_map_columns) is set to 1 and
dump contains CREATE query for specified table or column names in INSERT query the columns from input data will be mapped to the columns from the table by their names,
columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
This format supports schema inference: if the dump contains CREATE query for the specified table, the structure is extracted from it, otherwise schema is inferred from the data of INSERT queries.

Examples:

File dump.sql:
```sql
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test` (
  `x` int DEFAULT NULL,
  `y` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test` VALUES (1,NULL),(2,NULL),(3,NULL),(3,NULL),(4,NULL),(5,NULL),(6,7);
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test 3` (
  `y` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test 3` VALUES (1);
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test2` (
  `x` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test2` VALUES (1),(2),(3);
```

Queries:

```sql
DESCRIBE TABLE file(dump.sql, MySQLDump) SETTINGS input_format_mysql_dump_table_name = 'test2'
```

```text
┌─name─┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ x    │ Nullable(Int32) │              │                    │         │                  │                │
└──────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SELECT *
FROM file(dump.sql, MySQLDump)
         SETTINGS input_format_mysql_dump_table_name = 'test2'
```

```text
┌─x─┐
│ 1 │
│ 2 │
│ 3 │
└───┘
```

## DWARF {#dwarf}

Parses DWARF debug symbols from an ELF file (executable, library, or object file). Similar to `dwarfdump`, but much faster (hundreds of MB/s) and with SQL. Produces one row for each Debug Information Entry (DIE) in the `.debug_info` section. Includes "null" entries that the DWARF encoding uses to terminate lists of children in the tree.

Quick background: `.debug_info` consists of *units*, corresponding to compilation units. Each unit is a tree of *DIE*s, with a `compile_unit` DIE as its root. Each DIE has a *tag* and a list of *attributes*. Each attribute has a *name* and a *value* (and also a *form*, which specifies how the value is encoded). The DIEs represent things from the source code, and their *tag* tells what kind of thing it is. E.g. there are functions (tag = `subprogram`), classes/structs/enums (`class_type`/`structure_type`/`enumeration_type`), variables (`variable`), function arguments (`formal_parameter`). The tree structure mirrors the corresponding source code. E.g. a `class_type` DIE can contain `subprogram` DIEs representing methods of the class.

Outputs the following columns:
 - `offset` - position of the DIE in the `.debug_info` section
 - `size` - number of bytes in the encoded DIE (including attributes)
 - `tag` - type of the DIE; the conventional "DW_TAG_" prefix is omitted
 - `unit_name` - name of the compilation unit containing this DIE
 - `unit_offset` - position of the compilation unit containing this DIE in the `.debug_info` section
 - `ancestor_tags` - array of tags of the ancestors of the current DIE in the tree, in order from innermost to outermost
 - `ancestor_offsets` - offsets of ancestors, parallel to `ancestor_tags`
 - a few common attributes duplicated from the attributes array for convenience:
   - `name`
   - `linkage_name` - mangled fully-qualified name; typically only functions have it (but not all functions)
   - `decl_file` - name of the source code file where this entity was declared
   - `decl_line` - line number in the source code where this entity was declared
 - parallel arrays describing attributes:
   - `attr_name` - name of the attribute; the conventional "DW_AT_" prefix is omitted
   - `attr_form` - how the attribute is encoded and interpreted; the conventional DW_FORM_ prefix is omitted
   - `attr_int` - integer value of the attribute; 0 if the attribute doesn't have a numeric value
   - `attr_str` - string value of the attribute; empty if the attribute doesn't have a string value

Example: find compilation units that have the most function definitions (including template instantiations and functions from included header files):

```sql
SELECT
    unit_name,
    count() AS c
FROM file('programs/clickhouse', DWARF)
WHERE tag = 'subprogram' AND NOT has(attr_name, 'declaration')
GROUP BY unit_name
ORDER BY c DESC
LIMIT 3
```
```text
┌─unit_name──────────────────────────────────────────────────┬─────c─┐
│ ./src/Core/Settings.cpp                                    │ 28939 │
│ ./src/AggregateFunctions/AggregateFunctionSumMap.cpp       │ 23327 │
│ ./src/AggregateFunctions/AggregateFunctionUniqCombined.cpp │ 22649 │
└────────────────────────────────────────────────────────────┴───────┘

3 rows in set. Elapsed: 1.487 sec. Processed 139.76 million rows, 1.12 GB (93.97 million rows/s., 752.77 MB/s.)
Peak memory usage: 271.92 MiB.
```

## Markdown {#markdown}

You can export results using [Markdown](https://en.wikipedia.org/wiki/Markdown) format to generate output ready to be pasted into your `.md` files:

```sql
SELECT
    number,
    number * 2
FROM numbers(5)
FORMAT Markdown
```
```results
| number | multiply(number, 2) |
|-:|-:|
| 0 | 0 |
| 1 | 2 |
| 2 | 4 |
| 3 | 6 |
| 4 | 8 |
```

Markdown table will be generated automatically and can be used on markdown-enabled platforms, like Github. This format is used only for output.

## Form {#form}

The Form format can be used to read or write a single record in the application/x-www-form-urlencoded format in which data is formatted `key1=value1&key2=value2`

Examples:

Given a file `data.tmp` placed in the `user_files` path with some URL encoded data:

```text
t_page=116&c.e=ls7xfkpm&c.tti.m=raf&rt.start=navigation&rt.bmr=390%2C11%2C10
```

```sql
SELECT * FROM file(data.tmp, Form) FORMAT vertical;
```

Result:

```text
Row 1:
──────
t_page:   116
c.e:      ls7xfkpm
c.tti.m:  raf
rt.start: navigation
rt.bmr:   390,11,10
```
