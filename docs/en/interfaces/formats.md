---
slug: /en/interfaces/formats
sidebar_position: 21
sidebar_label: View all formats...
title: Formats for Input and Output Data
---

ClickHouse can accept and return data in various formats. A format supported for input can be used to parse the data provided to `INSERT`s, to perform `SELECT`s from a file-backed table such as File, URL or HDFS, or to read a dictionary. A format supported for output can be used to arrange the
results of a `SELECT`, and to perform `INSERT`s into a file-backed table.
All format names are case insensitive.

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
| [ProtobufList](#protobuflist)								    | ✔    | ✔     |
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

In TabSeparated format, data is written by row. Each row contains values separated by tabs. Each value is followed by a tab, except the last value in the row, which is followed by a line feed. Strictly Unix line feeds are assumed everywhere. The last row also must contain a line feed at the end. Values are written in text format, without enclosing quotation marks, and with special characters escaped.

This format is also available under the name `TSV`.

The `TabSeparated` format is convenient for processing data using custom programs and scripts. It is used by default in the HTTP interface, and in the command-line client’s batch mode. This format also allows transferring data between different DBMSs. For example, you can get a dump from MySQL and upload it to ClickHouse, or vice versa.

The `TabSeparated` format supports outputting total values (when using WITH TOTALS) and extreme values (when ‘extremes’ is set to 1). In these cases, the total values and extremes are output after the main data. The main result, total values, and extremes are separated from each other by an empty line. Example:

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated
```

``` response
2014-03-17      1406958
2014-03-18      1383658
2014-03-19      1405797
2014-03-20      1353623
2014-03-21      1245779
2014-03-22      1031592
2014-03-23      1046491

1970-01-01      8873898

2014-03-17      1031592
2014-03-23      1406958
```

### Data Formatting {#tabseparated-data-formatting}

Integer numbers are written in decimal form. Numbers can contain an extra “+” character at the beginning (ignored when parsing, and not recorded when formatting). Non-negative numbers can’t contain the negative sign. When reading, it is allowed to parse an empty string as a zero, or (for signed types) a string consisting of just a minus sign as a zero. Numbers that do not fit into the corresponding data type may be parsed as a different number, without an error message.

Floating-point numbers are written in decimal form. The dot is used as the decimal separator. Exponential entries are supported, as are ‘inf’, ‘+inf’, ‘-inf’, and ‘nan’. An entry of floating-point numbers may begin or end with a decimal point.
During formatting, accuracy may be lost on floating-point numbers.
During parsing, it is not strictly required to read the nearest machine-representable number.

Dates are written in YYYY-MM-DD format and parsed in the same format, but with any characters as separators.
Dates with times are written in the format `YYYY-MM-DD hh:mm:ss` and parsed in the same format, but with any characters as separators.
This all occurs in the system time zone at the time the client or server starts (depending on which of them formats data). For dates with times, daylight saving time is not specified. So if a dump has times during daylight saving time, the dump does not unequivocally match the data, and parsing will select one of the two times.
During a read operation, incorrect dates and dates with times can be parsed with natural overflow or as null dates and times, without an error message.

As an exception, parsing dates with times is also supported in Unix timestamp format, if it consists of exactly 10 decimal digits. The result is not time zone-dependent. The formats `YYYY-MM-DD hh:mm:ss` and `NNNNNNNNNN` are differentiated automatically.

Strings are output with backslash-escaped special characters. The following escape sequences are used for output: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. Parsing also supports the sequences `\a`, `\v`, and `\xHH` (hex escape sequences) and any `\c` sequences, where `c` is any character (these sequences are converted to `c`). Thus, reading data supports formats where a line feed can be written as `\n` or `\`, or as a line feed. For example, the string `Hello world` with a line feed between the words instead of space can be parsed in any of the following variations:

``` text
Hello\nworld

Hello\
world
```

The second variant is supported because MySQL uses it when writing tab-separated dumps.

The minimum set of characters that you need to escape when passing data in TabSeparated format: tab, line feed (LF) and backslash.

Only a small set of symbols are escaped. You can easily stumble onto a string value that your terminal will ruin in output.

Arrays are written as a list of comma-separated values in square brackets. Number items in the array are formatted as normally. `Date` and `DateTime` types are written in single quotes. Strings are written in single quotes with the same escaping rules as above.

[NULL](/docs/en/sql-reference/syntax.md) is formatted according to setting [format_tsv_null_representation](/docs/en/operations/settings/settings-formats.md/#format_tsv_null_representation) (default value is `\N`).

In input data, ENUM values can be represented as names or as ids. First, we try to match the input value to the ENUM name. If we fail and the input value is a number, we try to match this number to ENUM id.
If input data contains only ENUM ids, it's recommended to enable the setting [input_format_tsv_enum_as_number](/docs/en/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number) to optimize ENUM parsing.

Each element of [Nested](/docs/en/sql-reference/data-types/nested-data-structures/index.md) structures is represented as an array.

For example:

``` sql
CREATE TABLE nestedt
(
    `id` UInt8,
    `aux` Nested(
        a UInt8,
        b String
    )
)
ENGINE = TinyLog
```

``` sql
INSERT INTO nestedt Values ( 1, [1], ['a'])
```

``` sql
SELECT * FROM nestedt FORMAT TSV
```

``` response
1  [1]    ['a']
```

### TabSeparated format settings {#tabseparated-format-settings}

- [format_tsv_null_representation](/docs/en/operations/settings/settings-formats.md/#format_tsv_null_representation) - custom NULL representation in TSV format. Default value - `\N`.
- [input_format_tsv_empty_as_default](/docs/en/operations/settings/settings-formats.md/#input_format_tsv_empty_as_default) - treat empty fields in TSV input as default values. Default value - `false`. For complex default expressions [input_format_defaults_for_omitted_fields](/docs/en/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) must be enabled too.
- [input_format_tsv_enum_as_number](/docs/en/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number) - treat inserted enum values in TSV formats as enum indices. Default value - `false`.
- [input_format_tsv_use_best_effort_in_schema_inference](/docs/en/operations/settings/settings-formats.md/#input_format_tsv_use_best_effort_in_schema_inference) - use some tweaks and heuristics to infer schema in TSV format. If disabled, all fields will be inferred as Strings. Default value - `true`.
- [output_format_tsv_crlf_end_of_line](/docs/en/operations/settings/settings-formats.md/#output_format_tsv_crlf_end_of_line) - if it is set true, end of line in TSV output format will be `\r\n` instead of `\n`. Default value - `false`.
- [input_format_tsv_crlf_end_of_line](/docs/en/operations/settings/settings-formats.md/#input_format_tsv_crlf_end_of_line) - if it is set true, end of line in TSV input format will be `\r\n` instead of `\n`. Default value - `false`.
- [input_format_tsv_skip_first_lines](/docs/en/operations/settings/settings-formats.md/#input_format_tsv_skip_first_lines) - skip specified number of lines at the beginning of data. Default value - `0`.
- [input_format_tsv_detect_header](/docs/en/operations/settings/settings-formats.md/#input_format_tsv_detect_header) - automatically detect header with names and types in TSV format. Default value - `true`.
- [input_format_tsv_skip_trailing_empty_lines](/docs/en/operations/settings/settings-formats.md/#input_format_tsv_skip_trailing_empty_lines) - skip trailing empty lines at the end of data. Default value - `false`.
- [input_format_tsv_allow_variable_number_of_columns](/docs/en/operations/settings/settings-formats.md/#input_format_tsv_allow_variable_number_of_columns) - allow variable number of columns in TSV format, ignore extra columns and use default values on missing columns. Default value - `false`.

## TabSeparatedRaw {#tabseparatedraw}

Differs from `TabSeparated` format in that the rows are written without escaping.
When parsing with this format, tabs or linefeeds are not allowed in each field.

This format is also available under the names `TSVRaw`, `Raw`.

## TabSeparatedWithNames {#tabseparatedwithnames}

Differs from the `TabSeparated` format in that the column names are written in the first row.

During parsing, the first row is expected to contain the column names. You can use column names to determine their position and to check their correctness.

:::note
If setting [input_format_with_names_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from the input data will be mapped to the columns of the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
:::

This format is also available under the name `TSVWithNames`.

## TabSeparatedWithNamesAndTypes {#tabseparatedwithnamesandtypes}

Differs from the `TabSeparated` format in that the column names are written to the first row, while the column types are in the second row.

:::note
If setting [input_format_with_names_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from the input data will be mapped to the columns in the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
If setting [input_format_with_types_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to 1,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::

This format is also available under the name `TSVWithNamesAndTypes`.

## TabSeparatedRawWithNames {#tabseparatedrawwithnames}

Differs from `TabSeparatedWithNames` format in that the rows are written without escaping.
When parsing with this format, tabs or linefeeds are not allowed in each field.

This format is also available under the names `TSVRawWithNames`, `RawWithNames`.

## TabSeparatedRawWithNamesAndTypes {#tabseparatedrawwithnamesandtypes}

Differs from `TabSeparatedWithNamesAndTypes` format in that the rows are written without escaping.
When parsing with this format, tabs or linefeeds are not allowed in each field.

This format is also available under the names `TSVRawWithNamesAndNames`, `RawWithNamesAndNames`.

## Template {#format-template}

This format allows specifying a custom format string with placeholders for values with a specified escaping rule.

It uses settings `format_template_resultset`, `format_template_row` (`format_template_row_format`), `format_template_rows_between_delimiter` and some settings of other formats (e.g. `output_format_json_quote_64bit_integers` when using `JSON` escaping, see further)

Setting `format_template_row` specifies the path to the file containing format strings for rows with the following syntax:

`delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N`,

where `delimiter_i` is a delimiter between values (`$` symbol can be escaped as `$$`),
`column_i` is a name or index of a column whose values are to be selected or inserted (if empty, then column will be skipped),
`serializeAs_i` is an escaping rule for the column values. The following escaping rules are supported:

- `CSV`, `JSON`, `XML` (similar to the formats of the same names)
- `Escaped` (similar to `TSV`)
- `Quoted` (similar to `Values`)
- `Raw` (without escaping, similar to `TSVRaw`)
- `None` (no escaping rule, see further)

If an escaping rule is omitted, then `None` will be used. `XML` is suitable only for output.

So, for the following format string:

      `Search phrase: ${SearchPhrase:Quoted}, count: ${c:Escaped}, ad price: $$${price:JSON};`

the values of `SearchPhrase`, `c` and `price` columns, which are escaped as `Quoted`, `Escaped` and `JSON` will be printed (for select) or will be expected (for insert) between `Search phrase:`, `, count:`, `, ad price: $` and `;` delimiters respectively. For example:

`Search phrase: 'bathroom interior design', count: 2166, ad price: $3;`

In cases where it is challenging or not possible to deploy format output configuration for the template format to a directory on all nodes in a cluster, or if the format is trivial then `format_template_row_format` can be used to set the template string directly in the query, rather than a path to the file which contains it.

The `format_template_rows_between_delimiter` setting specifies the delimiter between rows, which is printed (or expected) after every row except the last one (`\n` by default)

Setting `format_template_resultset` specifies the path to the file, which contains a format string for resultset. Setting `format_template_resultset_format` can be used to set the template string for the result set directly in the query itself. Format string for resultset has the same syntax as a format string for row and allows to specify a prefix, a suffix and a way to print some additional information. It contains the following placeholders instead of column names:

- `data` is the rows with data in `format_template_row` format, separated by `format_template_rows_between_delimiter`. This placeholder must be the first placeholder in the format string.
- `totals` is the row with total values in `format_template_row` format (when using WITH TOTALS)
- `min` is the row with minimum values in `format_template_row` format (when extremes are set to 1)
- `max` is the row with maximum values in `format_template_row` format (when extremes are set to 1)
- `rows` is the total number of output rows
- `rows_before_limit` is the minimal number of rows there would have been without LIMIT. Output only if the query contains LIMIT. If the query contains GROUP BY, rows_before_limit_at_least is the exact number of rows there would have been without a LIMIT.
- `time` is the request execution time in seconds
- `rows_read` is the number of rows has been read
- `bytes_read` is the number of bytes (uncompressed) has been read

The placeholders `data`, `totals`, `min` and `max` must not have escaping rule specified (or `None` must be specified explicitly). The remaining placeholders may have any escaping rule specified.
If the `format_template_resultset` setting is an empty string, `${data}` is used as the default value.
For insert queries format allows skipping some columns or fields if prefix or suffix (see example).

Select example:

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5 FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = '\n    '
```

`/some/path/resultset.format`:

``` text
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    ${data}
  </table>
  <table border="1"> <caption>Max</caption>
    ${max}
  </table>
  <b>Processed ${rows_read:XML} rows in ${time:XML} sec</b>
 </body>
</html>
```

`/some/path/row.format`:

``` text
<tr> <td>${0:XML}</td> <td>${1:XML}</td> </tr>
```

Result:

``` html
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    <tr> <td></td> <td>8267016</td> </tr>
    <tr> <td>bathroom interior design</td> <td>2166</td> </tr>
    <tr> <td>clickhouse</td> <td>1655</td> </tr>
    <tr> <td>spring 2014 fashion</td> <td>1549</td> </tr>
    <tr> <td>freeform photos</td> <td>1480</td> </tr>
  </table>
  <table border="1"> <caption>Max</caption>
    <tr> <td></td> <td>8873898</td> </tr>
  </table>
  <b>Processed 3095973 rows in 0.1569913 sec</b>
 </body>
</html>
```

Insert example:

``` text
Some header
Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
Total rows: 2
```

``` sql
INSERT INTO UserActivity SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
FORMAT Template
```

`/some/path/resultset.format`:

``` text
Some header\n${data}\nTotal rows: ${:CSV}\n
```

`/some/path/row.format`:

``` text
Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}
```

`PageViews`, `UserID`, `Duration` and `Sign` inside placeholders are names of columns in the table. Values after `Useless field` in rows and after `\nTotal rows:` in suffix will be ignored.
All delimiters in the input data must be strictly equal to delimiters in specified format strings.

## TemplateIgnoreSpaces {#templateignorespaces}

This format is suitable only for input.
Similar to `Template`, but skips whitespace characters between delimiters and values in the input stream. However, if format strings contain whitespace characters, these characters will be expected in the input stream. Also allows specifying empty placeholders (`${}` or `${:None}`) to split some delimiter into separate parts to ignore spaces between them. Such placeholders are used only for skipping whitespace characters.
It’s possible to read `JSON` using this format if the values of columns have the same order in all rows. For example, the following request can be used for inserting data from its output example of format [JSON](#json):

``` sql
INSERT INTO table_name SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = ','
FORMAT TemplateIgnoreSpaces
```

`/some/path/resultset.format`:

``` text
{${}"meta"${}:${:JSON},${}"data"${}:${}[${data}]${},${}"totals"${}:${:JSON},${}"extremes"${}:${:JSON},${}"rows"${}:${:JSON},${}"rows_before_limit_at_least"${}:${:JSON}${}}
```

`/some/path/row.format`:

``` text
{${}"SearchPhrase"${}:${}${phrase:JSON}${},${}"c"${}:${}${cnt:JSON}${}}
```

## TSKV {#tskv}

Similar to TabSeparated, but outputs a value in name=value format. Names are escaped the same way as in TabSeparated format, and the = symbol is also escaped.

``` text
SearchPhrase=   count()=8267016
SearchPhrase=bathroom interior design    count()=2166
SearchPhrase=clickhouse     count()=1655
SearchPhrase=2014 spring fashion    count()=1549
SearchPhrase=freeform photos       count()=1480
SearchPhrase=angelina jolie    count()=1245
SearchPhrase=omsk       count()=1112
SearchPhrase=photos of dog breeds    count()=1091
SearchPhrase=curtain designs        count()=1064
SearchPhrase=baku       count()=1000
```

[NULL](/docs/en/sql-reference/syntax.md) is formatted as `\N`.

``` sql
SELECT * FROM t_null FORMAT TSKV
```

``` text
x=1    y=\N
```

When there is a large number of small columns, this format is ineffective, and there is generally no reason to use it. Nevertheless, it is no worse than JSONEachRow in terms of efficiency.

Both data output and parsing are supported in this format. For parsing, any order is supported for the values of different columns. It is acceptable for some values to be omitted – they are treated as equal to their default values. In this case, zeros and blank rows are used as default values. Complex values that could be specified in the table are not supported as defaults.

Parsing allows the presence of the additional field `tskv` without the equal sign or a value. This field is ignored.

During import, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.

## CSV {#csv}

Comma Separated Values format ([RFC](https://tools.ietf.org/html/rfc4180)).

When formatting, rows are enclosed in double quotes. A double quote inside a string is output as two double quotes in a row. There are no other rules for escaping characters. Date and date-time are enclosed in double quotes. Numbers are output without quotes. Values are separated by a delimiter character, which is `,` by default. The delimiter character is defined in the setting [format_csv_delimiter](/docs/en/operations/settings/settings-formats.md/#format_csv_delimiter). Rows are separated using the Unix line feed (LF). Arrays are serialized in CSV as follows: first, the array is serialized to a string as in TabSeparated format, and then the resulting string is output to CSV in double quotes. Tuples in CSV format are serialized as separate columns (that is, their nesting in the tuple is lost).

``` bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

\*By default, the delimiter is `,`. See the [format_csv_delimiter](/docs/en/operations/settings/settings-formats.md/#format_csv_delimiter) setting for more information.

When parsing, all values can be parsed either with or without quotes. Both double and single quotes are supported. Rows can also be arranged without quotes. In this case, they are parsed up to the delimiter character or line feed (CR or LF). In violation of the RFC, when parsing rows without quotes, the leading and trailing spaces and tabs are ignored. For the line feed, Unix (LF), Windows (CR LF) and Mac OS Classic (CR LF) types are all supported.

`NULL` is formatted according to setting [format_csv_null_representation](/docs/en/operations/settings/settings-formats.md/#format_csv_null_representation) (default value is `\N`).

In input data, ENUM values can be represented as names or as ids. First, we try to match the input value to the ENUM name. If we fail and the input value is a number, we try to match this number to the ENUM id.
If input data contains only ENUM ids, it's recommended to enable the setting [input_format_csv_enum_as_number](/docs/en/operations/settings/settings-formats.md/#input_format_csv_enum_as_number) to optimize ENUM parsing.

The CSV format supports the output of totals and extremes the same way as `TabSeparated`.

### CSV format settings {#csv-format-settings}

- [format_csv_delimiter](/docs/en/operations/settings/settings-formats.md/#format_csv_delimiter) - the character to be considered as a delimiter in CSV data. Default value - `,`.
- [format_csv_allow_single_quotes](/docs/en/operations/settings/settings-formats.md/#format_csv_allow_single_quotes) - allow strings in single quotes. Default value - `true`.
- [format_csv_allow_double_quotes](/docs/en/operations/settings/settings-formats.md/#format_csv_allow_double_quotes) - allow strings in double quotes. Default value - `true`.
- [format_csv_null_representation](/docs/en/operations/settings/settings-formats.md/#format_tsv_null_representation) - custom NULL representation in CSV format. Default value - `\N`.
- [input_format_csv_empty_as_default](/docs/en/operations/settings/settings-formats.md/#input_format_csv_empty_as_default) - treat empty fields in CSV input as default values. Default value - `true`. For complex default expressions, [input_format_defaults_for_omitted_fields](/docs/en/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) must be enabled too.
- [input_format_csv_enum_as_number](/docs/en/operations/settings/settings-formats.md/#input_format_csv_enum_as_number) - treat inserted enum values in CSV formats as enum indices. Default value - `false`.
- [input_format_csv_use_best_effort_in_schema_inference](/docs/en/operations/settings/settings-formats.md/#input_format_csv_use_best_effort_in_schema_inference) - use some tweaks and heuristics to infer schema in CSV format. If disabled, all fields will be inferred as Strings. Default value - `true`.
- [input_format_csv_arrays_as_nested_csv](/docs/en/operations/settings/settings-formats.md/#input_format_csv_arrays_as_nested_csv) - when reading Array from CSV, expect that its elements were serialized in nested CSV and then put into string. Default value - `false`.
- [output_format_csv_crlf_end_of_line](/docs/en/operations/settings/settings-formats.md/#output_format_csv_crlf_end_of_line) - if it is set to true, end of line in CSV output format will be `\r\n` instead of `\n`. Default value - `false`.
- [input_format_csv_skip_first_lines](/docs/en/operations/settings/settings-formats.md/#input_format_csv_skip_first_lines) - skip the specified number of lines at the beginning of data. Default value - `0`.
- [input_format_csv_detect_header](/docs/en/operations/settings/settings-formats.md/#input_format_csv_detect_header) - automatically detect header with names and types in CSV format. Default value - `true`.
- [input_format_csv_skip_trailing_empty_lines](/docs/en/operations/settings/settings-formats.md/#input_format_csv_skip_trailing_empty_lines) - skip trailing empty lines at the end of data. Default value - `false`.
- [input_format_csv_trim_whitespaces](/docs/en/operations/settings/settings-formats.md/#input_format_csv_trim_whitespaces) - trim spaces and tabs in non-quoted CSV strings. Default value - `true`.
- [input_format_csv_allow_whitespace_or_tab_as_delimiter](/docs/en/operations/settings/settings-formats.md/#input_format_csv_allow_whitespace_or_tab_as_delimiter) - Allow to use whitespace or tab as field delimiter in CSV strings. Default value - `false`.
- [input_format_csv_allow_variable_number_of_columns](/docs/en/operations/settings/settings-formats.md/#input_format_csv_allow_variable_number_of_columns) - allow variable number of columns in CSV format, ignore extra columns and use default values on missing columns. Default value - `false`.
- [input_format_csv_use_default_on_bad_values](/docs/en/operations/settings/settings-formats.md/#input_format_csv_use_default_on_bad_values) - Allow to set default value to column when CSV field deserialization failed on bad value. Default value - `false`.
- [input_format_csv_try_infer_numbers_from_strings](/docs/en/operations/settings/settings-formats.md/#input_format_csv_try_infer_numbers_from_strings) - Try to infer numbers from string fields while schema inference. Default value - `false`.

## CSVWithNames {#csvwithnames}

Also prints the header row with column names, similar to [TabSeparatedWithNames](#tabseparatedwithnames).

:::note
If setting [input_format_with_names_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
:::

## CSVWithNamesAndTypes {#csvwithnamesandtypes}

Also prints two header rows with column names and types, similar to [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes).

:::note
If setting [input_format_with_names_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
If setting [input_format_with_types_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to 1,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::

## CustomSeparated {#format-customseparated}

Similar to [Template](#format-template), but it prints or reads all names and types of columns and uses escaping rule from [format_custom_escaping_rule](/docs/en/operations/settings/settings-formats.md/#format_custom_escaping_rule) setting and delimiters from [format_custom_field_delimiter](/docs/en/operations/settings/settings-formats.md/#format_custom_field_delimiter), [format_custom_row_before_delimiter](/docs/en/operations/settings/settings-formats.md/#format_custom_row_before_delimiter), [format_custom_row_after_delimiter](/docs/en/operations/settings/settings-formats.md/#format_custom_row_after_delimiter), [format_custom_row_between_delimiter](/docs/en/operations/settings/settings-formats.md/#format_custom_row_between_delimiter), [format_custom_result_before_delimiter](/docs/en/operations/settings/settings-formats.md/#format_custom_result_before_delimiter) and [format_custom_result_after_delimiter](/docs/en/operations/settings/settings-formats.md/#format_custom_result_after_delimiter) settings, not from format strings.

Additional settings:
- [input_format_custom_detect_header](/docs/en/operations/settings/settings-formats.md/#input_format_custom_detect_header) - enables automatic detection of header with names and types if any. Default value - `true`.
- [input_format_custom_skip_trailing_empty_lines](/docs/en/operations/settings/settings-formats.md/#input_format_custom_skip_trailing_empty_lines) - skip trailing empty lines at the end of file . Default value - `false`.
- [input_format_custom_allow_variable_number_of_columns](/docs/en/operations/settings/settings-formats.md/#input_format_custom_allow_variable_number_of_columns) - allow variable number of columns in CustomSeparated format, ignore extra columns and use default values on missing columns. Default value - `false`.

There is also `CustomSeparatedIgnoreSpaces` format, which is similar to [TemplateIgnoreSpaces](#templateignorespaces).

## CustomSeparatedWithNames {#customseparatedwithnames}

Also prints the header row with column names, similar to [TabSeparatedWithNames](#tabseparatedwithnames).

:::note
If setting [input_format_with_names_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
:::

## CustomSeparatedWithNamesAndTypes {#customseparatedwithnamesandtypes}

Also prints two header rows with column names and types, similar to [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes).

:::note
If setting [input_format_with_names_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
If setting [input_format_with_types_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to 1,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::

## SQLInsert {#sqlinsert}

Outputs data as a sequence of `INSERT INTO table (columns...) VALUES (...), (...) ...;` statements.

Example:

```sql
SELECT number AS x, number + 1 AS y, 'Hello' AS z FROM numbers(10) FORMAT SQLInsert SETTINGS output_format_sql_insert_max_batch_size = 2
```

```sql
INSERT INTO table (x, y, z) VALUES (0, 1, 'Hello'), (1, 2, 'Hello');
INSERT INTO table (x, y, z) VALUES (2, 3, 'Hello'), (3, 4, 'Hello');
INSERT INTO table (x, y, z) VALUES (4, 5, 'Hello'), (5, 6, 'Hello');
INSERT INTO table (x, y, z) VALUES (6, 7, 'Hello'), (7, 8, 'Hello');
INSERT INTO table (x, y, z) VALUES (8, 9, 'Hello'), (9, 10, 'Hello');
```

To read data output by this format you can use [MySQLDump](#mysqldump) input format.

### SQLInsert format settings {#sqlinsert-format-settings}

- [output_format_sql_insert_max_batch_size](/docs/en/operations/settings/settings-formats.md/#output_format_sql_insert_max_batch_size) - The maximum number of rows in one INSERT statement. Default value - `65505`.
- [output_format_sql_insert_table_name](/docs/en/operations/settings/settings-formats.md/#output_format_sql_insert_table_name) - The name of the table in the output INSERT query. Default value - `'table'`.
- [output_format_sql_insert_include_column_names](/docs/en/operations/settings/settings-formats.md/#output_format_sql_insert_include_column_names) - Include column names in INSERT query. Default value - `true`.
- [output_format_sql_insert_use_replace](/docs/en/operations/settings/settings-formats.md/#output_format_sql_insert_use_replace) - Use REPLACE statement instead of INSERT. Default value - `false`.
- [output_format_sql_insert_quote_names](/docs/en/operations/settings/settings-formats.md/#output_format_sql_insert_quote_names) - Quote column names with "\`" characters. Default value - `true`.

## JSON {#json}

Outputs data in JSON format. Besides data tables, it also outputs column names and types, along with some additional information: the total number of output rows, and the number of rows that could have been output if there weren’t a LIMIT. Example:

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

``` json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },
                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        [
                {
                        "num": 42,
                        "str": "hello",
                        "arr": [0,1]
                },
                {
                        "num": 43,
                        "str": "hello",
                        "arr": [0,1,2]
                },
                {
                        "num": 44,
                        "str": "hello",
                        "arr": [0,1,2,3]
                }
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.001137687,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

The JSON is compatible with JavaScript. To ensure this, some characters are additionally escaped: the slash `/` is escaped as `\/`; alternative line breaks `U+2028` and `U+2029`, which break some browsers, are escaped as `\uXXXX`. ASCII control characters are escaped: backspace, form feed, line feed, carriage return, and horizontal tab are replaced with `\b`, `\f`, `\n`, `\r`, `\t` , as well as the remaining bytes in the 00-1F range using `\uXXXX` sequences. Invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences. For compatibility with JavaScript, Int64 and UInt64 integers are enclosed in double quotes by default. To remove the quotes, you can set the configuration parameter [output_format_json_quote_64bit_integers](/docs/en/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers) to 0.

`rows` – The total number of output rows.

`rows_before_limit_at_least` The minimal number of rows there would have been without LIMIT. Output only if the query contains LIMIT.
If the query contains GROUP BY, rows_before_limit_at_least is the exact number of rows there would have been without a LIMIT.

`totals` – Total values (when using WITH TOTALS).

`extremes` – Extreme values (when extremes are set to 1).

ClickHouse supports [NULL](/docs/en/sql-reference/syntax.md), which is displayed as `null` in the JSON output. To enable `+nan`, `-nan`, `+inf`, `-inf` values in output, set the [output_format_json_quote_denormals](/docs/en/operations/settings/settings-formats.md/#output_format_json_quote_denormals) to 1.

**See Also**

- [JSONEachRow](#jsoneachrow) format
- [output_format_json_array_of_rows](/docs/en/operations/settings/settings-formats.md/#output_format_json_array_of_rows) setting

For JSON input format, if setting [input_format_json_validate_types_from_metadata](/docs/en/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) is set to 1,
the types from metadata in input data will be compared with the types of the corresponding columns from the table.

## JSONStrings {#jsonstrings}

Differs from JSON only in that data fields are output in strings, not in typed JSON values.

Example:

```json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },
                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        [
                {
                        "num": "42",
                        "str": "hello",
                        "arr": "[0,1]"
                },
                {
                        "num": "43",
                        "str": "hello",
                        "arr": "[0,1,2]"
                },
                {
                        "num": "44",
                        "str": "hello",
                        "arr": "[0,1,2,3]"
                }
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.001403233,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

## JSONColumns {#jsoncolumns}

:::tip
The output of the JSONColumns* formats provides the ClickHouse field name and then the content of each row of the table for that field;
visually, the data is rotated 90 degrees to the left.
:::

In this format, all data is represented as a single JSON Object.
Note that JSONColumns output format buffers all data in memory to output it as a single block and it can lead to high memory consumption.

Example:
```json
{
	"num": [42, 43, 44],
	"str": ["hello", "hello", "hello"],
	"arr": [[0,1], [0,1,2], [0,1,2,3]]
}
```

During import, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Columns that are not present in the block will be filled with default values (you can use the [input_format_defaults_for_omitted_fields](/docs/en/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) setting here)


## JSONColumnsWithMetadata {#jsoncolumnsmonoblock}

Differs from JSONColumns format in that it also contains some metadata and statistics (similar to JSON format).
Output format buffers all data in memory and then outputs them as a single block, so, it can lead to high memory consumption.

Example:
```json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },

                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        {
                "num": [42, 43, 44],
                "str": ["hello", "hello", "hello"],
                "arr": [[0,1], [0,1,2], [0,1,2,3]]
        },

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.000272376,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

For JSONColumnsWithMetadata input format, if setting [input_format_json_validate_types_from_metadata](/docs/en/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) is set to 1,
the types from metadata in input data will be compared with the types of the corresponding columns from the table.

## JSONAsString {#jsonasstring}

In this format, a single JSON object is interpreted as a single value. If the input has several JSON objects (comma separated), they are interpreted as separate rows. If the input data is enclosed in square brackets, it is interpreted as an array of JSONs.

This format can only be parsed for a table with a single field of type [String](/docs/en/sql-reference/data-types/string.md). The remaining columns must be set to [DEFAULT](/docs/en/sql-reference/statements/create/table.md/#default) or [MATERIALIZED](/docs/en/sql-reference/statements/create/table.md/#materialized), or omitted. Once you collect the whole JSON object to string you can use [JSON functions](/docs/en/sql-reference/functions/json-functions.md) to process it.

**Examples**

Query:

``` sql
DROP TABLE IF EXISTS json_as_string;
CREATE TABLE json_as_string (json String) ENGINE = Memory;
INSERT INTO json_as_string (json) FORMAT JSONAsString {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_string;
```

Result:

``` response
┌─json──────────────────────────────┐
│ {"foo":{"bar":{"x":"y"},"baz":1}} │
│ {}                                │
│ {"any json stucture":1}           │
└───────────────────────────────────┘
```

**An array of JSON objects**

Query:

``` sql
CREATE TABLE json_square_brackets (field String) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT JSONAsString [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];

SELECT * FROM json_square_brackets;
```

Result:

```response
┌─field──────────────────────┐
│ {"id": 1, "name": "name1"} │
│ {"id": 2, "name": "name2"} │
└────────────────────────────┘
```

## JSONAsObject {#jsonasobject}

In this format, a single JSON object is interpreted as a single [JSON](/docs/en/sql-reference/data-types/newjson.md) value. If the input has several JSON objects (comma separated), they are interpreted as separate rows. If the input data is enclosed in square brackets, it is interpreted as an array of JSONs.

This format can only be parsed for a table with a single field of type [JSON](/docs/en/sql-reference/data-types/newjson.md). The remaining columns must be set to [DEFAULT](/docs/en/sql-reference/statements/create/table.md/#default) or [MATERIALIZED](/docs/en/sql-reference/statements/create/table.md/#materialized).

**Examples**

Query:

``` sql
SET allow_experimental_json_type = 1;
CREATE TABLE json_as_object (json JSON) ENGINE = Memory;
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_object FORMAT JSONEachRow;
```

Result:

``` response
{"json":{"foo":{"bar":{"x":"y"},"baz":"1"}}}
{"json":{}}
{"json":{"any json stucture":"1"}}
```

**An array of JSON objects**

Query:

``` sql
SET allow_experimental_json_type = 1;
CREATE TABLE json_square_brackets (field JSON) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT JSONAsObject [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];
SELECT * FROM json_square_brackets FORMAT JSONEachRow;
```

Result:

```response
{"field":{"id":"1","name":"name1"}}
{"field":{"id":"2","name":"name2"}}
```

**Columns with default values**

```sql
SET allow_experimental_json_type = 1;
CREATE TABLE json_as_object (json JSON, time DateTime MATERIALIZED now()) ENGINE = Memory;
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"foo":{"bar":{"x":"y"},"baz":1}};
INSERT INTO json_as_object (json) FORMAT JSONAsObject {};
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"any json stucture":1}
SELECT time, json FROM json_as_object FORMAT JSONEachRow
```

```response
{"time":"2024-09-16 12:18:10","json":{}}
{"time":"2024-09-16 12:18:13","json":{"any json stucture":"1"}}
{"time":"2024-09-16 12:18:08","json":{"foo":{"bar":{"x":"y"},"baz":"1"}}}
```

## JSONCompact {#jsoncompact}

Differs from JSON only in that data rows are output in arrays, not in objects.

Example:

```json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },
                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        [
                [42, "hello", [0,1]],
                [43, "hello", [0,1,2]],
                [44, "hello", [0,1,2,3]]
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.001222069,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

## JSONCompactStrings {#jsoncompactstrings}

Differs from JSONStrings only in that data rows are output in arrays, not in objects.

Example:
f
```json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },
                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        [
                ["42", "hello", "[0,1]"],
                ["43", "hello", "[0,1,2]"],
                ["44", "hello", "[0,1,2,3]"]
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.001572097,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

## JSONCompactColumns {#jsoncompactcolumns}

In this format, all data is represented as a single JSON Array.
Note that JSONCompactColumns output format buffers all data in memory to output it as a single block and it can lead to high memory consumption

Example:
```json
[
	[42, 43, 44],
	["hello", "hello", "hello"],
	[[0,1], [0,1,2], [0,1,2,3]]
]
```

Columns that are not present in the block will be filled with default values (you can use  [input_format_defaults_for_omitted_fields](/docs/en/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) setting here)

## JSONEachRow {#jsoneachrow}

In this format, ClickHouse outputs each row as a separated, newline-delimited JSON Object.

Example:

```json
{"num":42,"str":"hello","arr":[0,1]}
{"num":43,"str":"hello","arr":[0,1,2]}
{"num":44,"str":"hello","arr":[0,1,2,3]}
```

While importing data columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.

## PrettyJSONEachRow {#prettyjsoneachrow}

Differs from JSONEachRow only in that JSON is pretty formatted with new line delimiters and 4 space indents. Suitable only for output.

Example

```json
{
    "num": "42",
    "str": "hello",
    "arr": [
        "0",
        "1"
    ],
    "tuple": {
        "num": 42,
        "str": "world"
    }
}
{
    "num": "43",
    "str": "hello",
    "arr": [
        "0",
        "1",
        "2"
    ],
    "tuple": {
        "num": 43,
        "str": "world"
    }
}
```

## JSONStringsEachRow {#jsonstringseachrow}

Differs from JSONEachRow only in that data fields are output in strings, not in typed JSON values.

Example:

```json
{"num":"42","str":"hello","arr":"[0,1]"}
{"num":"43","str":"hello","arr":"[0,1,2]"}
{"num":"44","str":"hello","arr":"[0,1,2,3]"}
```

## JSONCompactEachRow {#jsoncompacteachrow}

Differs from JSONEachRow only in that data rows are output in arrays, not in objects.

Example:

```json
[42, "hello", [0,1]]
[43, "hello", [0,1,2]]
[44, "hello", [0,1,2,3]]
```

## JSONCompactStringsEachRow {#jsoncompactstringseachrow}

Differs from JSONCompactEachRow only in that data fields are output in strings, not in typed JSON values.

Example:

```json
["42", "hello", "[0,1]"]
["43", "hello", "[0,1,2]"]
["44", "hello", "[0,1,2,3]"]
```

## JSONEachRowWithProgress {#jsoneachrowwithprogress}
## JSONStringsEachRowWithProgress {#jsonstringseachrowwithprogress}

Differs from `JSONEachRow`/`JSONStringsEachRow` in that ClickHouse will also yield progress information as JSON values.

```json
{"row":{"num":42,"str":"hello","arr":[0,1]}}
{"row":{"num":43,"str":"hello","arr":[0,1,2]}}
{"row":{"num":44,"str":"hello","arr":[0,1,2,3]}}
{"progress":{"read_rows":"3","read_bytes":"24","written_rows":"0","written_bytes":"0","total_rows_to_read":"3"}}
```

## JSONCompactEachRowWithNames {#jsoncompacteachrowwithnames}

Differs from `JSONCompactEachRow` format in that it also prints the header row with column names, similar to [TabSeparatedWithNames](#tabseparatedwithnames).

:::note
If setting [input_format_with_names_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
:::

## JSONCompactEachRowWithNamesAndTypes {#jsoncompacteachrowwithnamesandtypes}

Differs from `JSONCompactEachRow` format in that it also prints two header rows with column names and types, similar to [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes).

:::note
If setting [input_format_with_names_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
If setting [input_format_with_types_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to 1,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::

## JSONCompactStringsEachRowWithNames {#jsoncompactstringseachrowwithnames}

Differs from `JSONCompactStringsEachRow` in that in that it also prints the header row with column names, similar to [TabSeparatedWithNames](#tabseparatedwithnames).

:::note
If setting [input_format_with_names_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
:::

## JSONCompactStringsEachRowWithNamesAndTypes {#jsoncompactstringseachrowwithnamesandtypes}

Differs from `JSONCompactStringsEachRow` in that it also prints two header rows with column names and types, similar to [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes).

:::note
If setting [input_format_with_names_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
If setting [input_format_with_types_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to 1,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::

```json
["num", "str", "arr"]
["Int32", "String", "Array(UInt8)"]
[42, "hello", [0,1]]
[43, "hello", [0,1,2]]
[44, "hello", [0,1,2,3]]
```

## JSONObjectEachRow {#jsonobjecteachrow}

In this format, all data is represented as a single JSON Object, each row is represented as a separate field of this object similar to JSONEachRow format.

Example:

```json
{
  "row_1": {"num": 42, "str": "hello", "arr":  [0,1]},
  "row_2": {"num": 43, "str": "hello", "arr":  [0,1,2]},
  "row_3": {"num": 44, "str": "hello", "arr":  [0,1,2,3]}
}
```

To use an object name as a column value you can use the special setting [format_json_object_each_row_column_for_object_name](/docs/en/operations/settings/settings-formats.md/#format_json_object_each_row_column_for_object_name). The value of this setting is set to the name of a column, that is used as JSON key for a row in the resulting object.
Examples:

For output:

Let's say we have the table `test` with two columns:
```
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```
Let's output it in `JSONObjectEachRow` format and use `format_json_object_each_row_column_for_object_name` setting:

```sql
select * from test settings format_json_object_each_row_column_for_object_name='object_name'
```

The output:
```json
{
	"first_obj": {"number": 1},
	"second_obj": {"number": 2},
	"third_obj": {"number": 3}
}
```

For input:

Let's say we stored output from the previous example in a file named `data.json`:
```sql
select * from file('data.json', JSONObjectEachRow, 'object_name String, number UInt64') settings format_json_object_each_row_column_for_object_name='object_name'
```

```
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

It also works in schema inference:

```sql
desc file('data.json', JSONObjectEachRow) settings format_json_object_each_row_column_for_object_name='object_name'
```

```
┌─name────────┬─type────────────┐
│ object_name │ String          │
│ number      │ Nullable(Int64) │
└─────────────┴─────────────────┘
```


### Inserting Data {#json-inserting-data}

``` sql
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

ClickHouse allows:

- Any order of key-value pairs in the object.
- Omitting some values.

ClickHouse ignores spaces between elements and commas after the objects. You can pass all the objects in one line. You do not have to separate them with line breaks.

**Omitted values processing**

ClickHouse substitutes omitted values with the default values for the corresponding [data types](/docs/en/sql-reference/data-types/index.md).

If `DEFAULT expr` is specified, ClickHouse uses different substitution rules depending on the [input_format_defaults_for_omitted_fields](/docs/en/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) setting.

Consider the following table:

``` sql
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

- If `input_format_defaults_for_omitted_fields = 0`, then the default value for `x` and `a` equals `0` (as the default value for the `UInt32` data type).
- If `input_format_defaults_for_omitted_fields = 1`, then the default value for `x` equals `0`, but the default value of `a` equals `x * 2`.

:::note
When inserting data with `input_format_defaults_for_omitted_fields = 1`, ClickHouse consumes more computational resources, compared to insertion with `input_format_defaults_for_omitted_fields = 0`.
:::

### Selecting Data {#json-selecting-data}

Consider the `UserActivity` table as an example:

``` response
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

The query `SELECT * FROM UserActivity FORMAT JSONEachRow` returns:

``` response
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

Unlike the [JSON](#json) format, there is no substitution of invalid UTF-8 sequences. Values are escaped in the same way as for `JSON`.

:::info
Any set of bytes can be output in the strings. Use the `JSONEachRow` format if you are sure that the data in the table can be formatted as JSON without losing any information.
:::

### Usage of Nested Structures {#jsoneachrow-nested}

If you have a table with [Nested](/docs/en/sql-reference/data-types/nested-data-structures/index.md) data type columns, you can insert JSON data with the same structure. Enable this feature with the [input_format_import_nested_json](/docs/en/operations/settings/settings-formats.md/#input_format_import_nested_json) setting.

For example, consider the following table:

``` sql
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

As you can see in the `Nested` data type description, ClickHouse treats each component of the nested structure as a separate column (`n.s` and `n.i` for our table). You can insert data in the following way:

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

To insert data as a hierarchical JSON object, set [input_format_import_nested_json=1](/docs/en/operations/settings/settings-formats.md/#input_format_import_nested_json).

``` json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

Without this setting, ClickHouse throws an exception.

``` sql
SELECT name, value FROM system.settings WHERE name = 'input_format_import_nested_json'
```

``` response
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

``` response
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

``` sql
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

``` response
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

### JSON formats settings {#json-formats-settings}

- [input_format_import_nested_json](/docs/en/operations/settings/settings-formats.md/#input_format_import_nested_json) - map nested JSON data to nested tables (it works for JSONEachRow format). Default value - `false`.
- [input_format_json_read_bools_as_numbers](/docs/en/operations/settings/settings-formats.md/#input_format_json_read_bools_as_numbers) - allow to parse bools as numbers in JSON input formats. Default value - `true`.
- [input_format_json_read_bools_as_strings](/docs/en/operations/settings/settings-formats.md/#input_format_json_read_bools_as_strings) - allow to parse bools as strings in JSON input formats. Default value - `true`.
- [input_format_json_read_numbers_as_strings](/docs/en/operations/settings/settings-formats.md/#input_format_json_read_numbers_as_strings) - allow to parse numbers as strings in JSON input formats. Default value - `true`.
- [input_format_json_read_arrays_as_strings](/docs/en/operations/settings/settings-formats.md/#input_format_json_read_arrays_as_strings) - allow to parse JSON arrays as strings in JSON input formats. Default value - `true`.
- [input_format_json_read_objects_as_strings](/docs/en/operations/settings/settings-formats.md/#input_format_json_read_objects_as_strings) - allow to parse JSON objects as strings in JSON input formats. Default value - `true`.
- [input_format_json_named_tuples_as_objects](/docs/en/operations/settings/settings-formats.md/#input_format_json_named_tuples_as_objects) - parse named tuple columns as JSON objects. Default value - `true`.
- [input_format_json_try_infer_numbers_from_strings](/docs/en/operations/settings/settings-formats.md/#input_format_json_try_infer_numbers_from_strings) - try to infer numbers from string fields while schema inference. Default value - `false`.
- [input_format_json_try_infer_named_tuples_from_objects](/docs/en/operations/settings/settings-formats.md/#input_format_json_try_infer_named_tuples_from_objects) - try to infer named tuple from JSON objects during schema inference. Default value - `true`.
- [input_format_json_infer_incomplete_types_as_strings](/docs/en/operations/settings/settings-formats.md/#input_format_json_infer_incomplete_types_as_strings) - use type String for keys that contains only Nulls or empty objects/arrays during schema inference in JSON input formats. Default value - `true`.
- [input_format_json_defaults_for_missing_elements_in_named_tuple](/docs/en/operations/settings/settings-formats.md/#input_format_json_defaults_for_missing_elements_in_named_tuple) - insert default values for missing elements in JSON object while parsing named tuple. Default value - `true`.
- [input_format_json_ignore_unknown_keys_in_named_tuple](/docs/en/operations/settings/settings-formats.md/#input_format_json_ignore_unknown_keys_in_named_tuple) - ignore unknown keys in json object for named tuples. Default value - `false`.
- [input_format_json_compact_allow_variable_number_of_columns](/docs/en/operations/settings/settings-formats.md/#input_format_json_compact_allow_variable_number_of_columns) - allow variable number of columns in JSONCompact/JSONCompactEachRow format, ignore extra columns and use default values on missing columns. Default value - `false`.
- [input_format_json_throw_on_bad_escape_sequence](/docs/en/operations/settings/settings-formats.md/#input_format_json_throw_on_bad_escape_sequence) - throw an exception if JSON string contains bad escape sequence. If disabled, bad escape sequences will remain as is in the data. Default value - `true`.
- [input_format_json_empty_as_default](/docs/en/operations/settings/settings-formats.md/#input_format_json_empty_as_default) - treat empty fields in JSON input as default values. Default value - `false`. For complex default expressions [input_format_defaults_for_omitted_fields](/docs/en/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) must be enabled too.
- [output_format_json_quote_64bit_integers](/docs/en/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers) - controls quoting of 64-bit integers in JSON output format. Default value - `true`.
- [output_format_json_quote_64bit_floats](/docs/en/operations/settings/settings-formats.md/#output_format_json_quote_64bit_floats) - controls quoting of 64-bit floats in JSON output format. Default value - `false`.
- [output_format_json_quote_denormals](/docs/en/operations/settings/settings-formats.md/#output_format_json_quote_denormals) - enables '+nan', '-nan', '+inf', '-inf' outputs in JSON output format. Default value - `false`.
- [output_format_json_quote_decimals](/docs/en/operations/settings/settings-formats.md/#output_format_json_quote_decimals) - controls quoting of decimals in JSON output format. Default value - `false`.
- [output_format_json_escape_forward_slashes](/docs/en/operations/settings/settings-formats.md/#output_format_json_escape_forward_slashes) - controls escaping forward slashes for string outputs in JSON output format. Default value - `true`.
- [output_format_json_named_tuples_as_objects](/docs/en/operations/settings/settings-formats.md/#output_format_json_named_tuples_as_objects) - serialize named tuple columns as JSON objects. Default value - `true`.
- [output_format_json_array_of_rows](/docs/en/operations/settings/settings-formats.md/#output_format_json_array_of_rows) - output a JSON array of all rows in JSONEachRow(Compact) format. Default value - `false`.
- [output_format_json_validate_utf8](/docs/en/operations/settings/settings-formats.md/#output_format_json_validate_utf8) - enables validation of UTF-8 sequences in JSON output formats (note that it doesn't impact formats JSON/JSONCompact/JSONColumnsWithMetadata, they always validate utf8). Default value - `false`.

## BSONEachRow {#bsoneachrow}

In this format, ClickHouse formats/parses data as a sequence of BSON documents without any separator between them.
Each row is formatted as a single document and each column is formatted as a single BSON document field with column name as a key.

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

Note: this format don't work properly on Big-Endian platforms.

### BSON format settings {#bson-format-settings}

- [output_format_bson_string_as_string](/docs/en/operations/settings/settings-formats.md/#output_format_bson_string_as_string) - use BSON String type instead of Binary for String columns. Default value - `false`.
- [input_format_bson_skip_fields_with_unsupported_types_in_schema_inference](/docs/en/operations/settings/settings-formats.md/#input_format_bson_skip_fields_with_unsupported_types_in_schema_inference) - allow skipping columns with unsupported types while schema inference for format BSONEachRow. Default value - `false`.

## Native {#native}

The most efficient format. Data is written and read by blocks in binary format. For each block, the number of rows, number of columns, column names and types, and parts of columns in this block are recorded one after another. In other words, this format is “columnar” – it does not convert columns to rows. This is the format used in the native interface for interaction between servers, for using the command-line client, and for C++ clients.

You can use this format to quickly generate dumps that can only be read by the ClickHouse DBMS. It does not make sense to work with this format yourself.

## Null {#null}

Nothing is output. However, the query is processed, and when using the command-line client, data is transmitted to the client. This is used for tests, including performance testing.
Obviously, this format is only appropriate for output, not for parsing.

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

## PrettyCompactNoEscapes {#prettynoescapes}

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

Prints every row in brackets. Rows are separated by commas. There is no comma after the last row. The values inside the brackets are also comma-separated. Numbers are output in a decimal format without quotes. Arrays are output in square brackets. Strings, dates, and dates with times are output in quotes. Escaping rules and parsing are similar to the [TabSeparated](#tabseparated) format. During formatting, extra spaces aren’t inserted, but during parsing, they are allowed and skipped (except for spaces inside array values, which are not allowed). [NULL](/docs/en/sql-reference/syntax.md) is represented as `NULL`.

The minimum set of characters that you need to escape when passing data in Values ​​format: single quotes and backslashes.

This is the format that is used in `INSERT INTO t VALUES ...`, but you can also use it for formatting query results.

## Values format settings {#values-format-settings}

- [input_format_values_interpret_expressions](/docs/en/operations/settings/settings-formats.md/#input_format_values_interpret_expressions) - if the field could not be parsed by streaming parser, run SQL parser and try to interpret it as SQL expression. Default value - `true`.
- [input_format_values_deduce_templates_of_expressions](/docs/en/operations/settings/settings-formats.md/#input_format_values_deduce_templates_of_expressions) -if the field could not be parsed by streaming parser, run SQL parser, deduce template of the SQL expression, try to parse all rows using template and then interpret expression for all rows. Default value - `true`.
- [input_format_values_accurate_types_of_literals](/docs/en/operations/settings/settings-formats.md/#input_format_values_accurate_types_of_literals) - when parsing and interpreting expressions using template, check actual type of literal to avoid possible overflow and precision issues. Default value - `true`.


## Vertical {#vertical}

Prints each value on a separate line with the column name specified. This format is convenient for printing just one or a few rows if each row consists of a large number of columns.

[NULL](/docs/en/sql-reference/syntax.md) is output as `ᴺᵁᴸᴸ`.

Example:

``` sql
SELECT * FROM t_null FORMAT Vertical
```

``` response
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

Rows are not escaped in Vertical format:

``` sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

``` response
Row 1:
──────
test: string with 'quotes' and      with some special
 characters
```

This format is only appropriate for outputting a query result, but not for parsing (retrieving data to insert in a table).

## XML {#xml}

XML format is suitable only for output, not for parsing. Example:

``` xml
<?xml version='1.0' encoding='UTF-8' ?>
<result>
        <meta>
                <columns>
                        <column>
                                <name>SearchPhrase</name>
                                <type>String</type>
                        </column>
                        <column>
                                <name>count()</name>
                                <type>UInt64</type>
                        </column>
                </columns>
        </meta>
        <data>
                <row>
                        <SearchPhrase></SearchPhrase>
                        <field>8267016</field>
                </row>
                <row>
                        <SearchPhrase>bathroom interior design</SearchPhrase>
                        <field>2166</field>
                </row>
                <row>
                        <SearchPhrase>clickhouse</SearchPhrase>
                        <field>1655</field>
                </row>
                <row>
                        <SearchPhrase>2014 spring fashion</SearchPhrase>
                        <field>1549</field>
                </row>
                <row>
                        <SearchPhrase>freeform photos</SearchPhrase>
                        <field>1480</field>
                </row>
                <row>
                        <SearchPhrase>angelina jolie</SearchPhrase>
                        <field>1245</field>
                </row>
                <row>
                        <SearchPhrase>omsk</SearchPhrase>
                        <field>1112</field>
                </row>
                <row>
                        <SearchPhrase>photos of dog breeds</SearchPhrase>
                        <field>1091</field>
                </row>
                <row>
                        <SearchPhrase>curtain designs</SearchPhrase>
                        <field>1064</field>
                </row>
                <row>
                        <SearchPhrase>baku</SearchPhrase>
                        <field>1000</field>
                </row>
        </data>
        <rows>10</rows>
        <rows_before_limit_at_least>141137</rows_before_limit_at_least>
</result>
```

If the column name does not have an acceptable format, just ‘field’ is used as the element name. In general, the XML structure follows the JSON structure.
Just as for JSON, invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences.

In string values, the characters `<` and `&` are escaped as `<` and `&`.

Arrays are output as `<array><elem>Hello</elem><elem>World</elem>...</array>`,and tuples as `<tuple><elem>Hello</elem><elem>World</elem>...</tuple>`.

## CapnProto {#capnproto}

CapnProto is a binary message format similar to [Protocol Buffers](https://developers.google.com/protocol-buffers/) and [Thrift](https://en.wikipedia.org/wiki/Apache_Thrift), but not like [JSON](#json) or [MessagePack](https://msgpack.org/).

CapnProto messages are strictly typed and not self-describing, meaning they need an external schema description. The schema is applied on the fly and cached for each query.

See also [Format Schema](#formatschema).

### Data Types Matching {#data_types-matching-capnproto}

The table below shows supported data types and how they match ClickHouse [data types](/docs/en/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| CapnProto data type (`INSERT`)                       | ClickHouse data type                                                                                                                                                           | CapnProto data type (`SELECT`)                       |
|------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|
| `UINT8`, `BOOL`                                      | [UInt8](/docs/en/sql-reference/data-types/int-uint.md)                                                                                                                         | `UINT8`                                              |
| `INT8`                                               | [Int8](/docs/en/sql-reference/data-types/int-uint.md)                                                                                                                          | `INT8`                                               |
| `UINT16`                                             | [UInt16](/docs/en/sql-reference/data-types/int-uint.md), [Date](/docs/en/sql-reference/data-types/date.md)                                                                     | `UINT16`                                             |
| `INT16`                                              | [Int16](/docs/en/sql-reference/data-types/int-uint.md)                                                                                                                         | `INT16`                                              |
| `UINT32`                                             | [UInt32](/docs/en/sql-reference/data-types/int-uint.md), [DateTime](/docs/en/sql-reference/data-types/datetime.md)                                                             | `UINT32`                                             |
| `INT32`                                              | [Int32](/docs/en/sql-reference/data-types/int-uint.md), [Decimal32](/docs/en/sql-reference/data-types/decimal.md)                                                              | `INT32`                                              |
| `UINT64`                                             | [UInt64](/docs/en/sql-reference/data-types/int-uint.md)                                                                                                                        | `UINT64`                                             |
| `INT64`                                              | [Int64](/docs/en/sql-reference/data-types/int-uint.md), [DateTime64](/docs/en/sql-reference/data-types/datetime.md), [Decimal64](/docs/en/sql-reference/data-types/decimal.md) | `INT64`                                              |
| `FLOAT32`                                            | [Float32](/docs/en/sql-reference/data-types/float.md)                                                                                                                          | `FLOAT32`                                            |
| `FLOAT64`                                            | [Float64](/docs/en/sql-reference/data-types/float.md)                                                                                                                          | `FLOAT64`                                            |
| `TEXT, DATA`                                         | [String](/docs/en/sql-reference/data-types/string.md), [FixedString](/docs/en/sql-reference/data-types/fixedstring.md)                                                         | `TEXT, DATA`                                         |
| `union(T, Void), union(Void, T)`                     | [Nullable(T)](/docs/en/sql-reference/data-types/date.md)                                                                                                                       | `union(T, Void), union(Void, T)`                     |
| `ENUM`                                               | [Enum(8/16)](/docs/en/sql-reference/data-types/enum.md)                                                                                                                        | `ENUM`                                               |
| `LIST`                                               | [Array](/docs/en/sql-reference/data-types/array.md)                                                                                                                            | `LIST`                                               |
| `STRUCT`                                             | [Tuple](/docs/en/sql-reference/data-types/tuple.md)                                                                                                                            | `STRUCT`                                             |
| `UINT32`                                             | [IPv4](/docs/en/sql-reference/data-types/ipv4.md)                                                                                                                              | `UINT32`                                             |
| `DATA`                                               | [IPv6](/docs/en/sql-reference/data-types/ipv6.md)                                                                                                                              | `DATA`                                               |
| `DATA`                                               | [Int128/UInt128/Int256/UInt256](/docs/en/sql-reference/data-types/int-uint.md)                                                                                                 | `DATA`                                               |
| `DATA`                                               | [Decimal128/Decimal256](/docs/en/sql-reference/data-types/decimal.md)                                                                                                          | `DATA`                                               |
| `STRUCT(entries LIST(STRUCT(key Key, value Value)))` | [Map](/docs/en/sql-reference/data-types/map.md)                                                                                                                                | `STRUCT(entries LIST(STRUCT(key Key, value Value)))` |

Integer types can be converted into each other during input/output.

For working with `Enum` in CapnProto format use the [format_capn_proto_enum_comparising_mode](/docs/en/operations/settings/settings-formats.md/#format_capn_proto_enum_comparising_mode) setting.

Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types also can be nested.

### Inserting and Selecting Data {#inserting-and-selecting-data-capnproto}

You can insert CapnProto data from a file into ClickHouse table by the following command:

``` bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_schema = 'schema:Message' FORMAT CapnProto"
```

Where `schema.capnp` looks like this:

``` capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

You can select data from a ClickHouse table and save them into some file in the CapnProto format by the following command:

``` bash
$ clickhouse-client --query = "SELECT * FROM test.hits FORMAT CapnProto SETTINGS format_schema = 'schema:Message'"
```

### Using autogenerated schema {#using-autogenerated-capn-proto-schema}

If you don't have an external CapnProto schema for your data, you can still output/input data in CapnProto format using autogenerated schema.
For example:

```sql
SELECT * FROM test.hits format CapnProto SETTINGS format_capn_proto_use_autogenerated_schema=1
```

In this case ClickHouse will autogenerate CapnProto schema according to the table structure using function [structureToCapnProtoSchema](../sql-reference/functions/other-functions.md#structure_to_capn_proto_schema) and will use this schema to serialize data in CapnProto format.

You can also read CapnProto file with autogenerated schema (in this case the file must be created using the same schema):

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_capn_proto_use_autogenerated_schema=1 FORMAT CapnProto"
```

The setting [format_capn_proto_use_autogenerated_schema](../operations/settings/settings-formats.md#format_capn_proto_use_autogenerated_schema) is enabled by default and applies if [format_schema](../operations/settings/settings-formats.md#formatschema-format-schema) is not set.

You can also save autogenerated schema in the file during input/output using setting [output_format_schema](../operations/settings/settings-formats.md#outputformatschema-output-format-schema). For example:

```sql
SELECT * FROM test.hits format CapnProto SETTINGS format_capn_proto_use_autogenerated_schema=1, output_format_schema='path/to/schema/schema.capnp'
```

In this case autogenerated CapnProto schema will be saved in file `path/to/schema/schema.capnp`.

## Prometheus {#prometheus}

Expose metrics in [Prometheus text-based exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/#text-based-format).

The output table should have a proper structure.
Columns `name` ([String](/docs/en/sql-reference/data-types/string.md)) and `value` (number) are required.
Rows may optionally contain `help` ([String](/docs/en/sql-reference/data-types/string.md)) and `timestamp` (number).
Column `type` ([String](/docs/en/sql-reference/data-types/string.md)) is either `counter`, `gauge`, `histogram`, `summary`, `untyped` or empty.
Each metric value may also have some `labels` ([Map(String, String)](/docs/en/sql-reference/data-types/map.md)).
Several consequent rows may refer to the one metric with different labels. The table should be sorted by metric name (e.g., with `ORDER BY name`).

There's special requirements for labels for `histogram` and `summary`, see [Prometheus doc](https://prometheus.io/docs/instrumenting/exposition_formats/#histograms-and-summaries) for the details. Special rules applied to row with labels `{'count':''}` and `{'sum':''}`, they'll be converted to `<metric_name>_count` and `<metric_name>_sum` respectively.

**Example:**

```
┌─name────────────────────────────────┬─type──────┬─help──────────────────────────────────────┬─labels─────────────────────────┬────value─┬─────timestamp─┐
│ http_request_duration_seconds       │ histogram │ A histogram of the request duration.      │ {'le':'0.05'}                  │    24054 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'0.1'}                   │    33444 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'0.2'}                   │   100392 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'0.5'}                   │   129389 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'1'}                     │   133988 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'+Inf'}                  │   144320 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'sum':''}                     │    53423 │             0 │
│ http_requests_total                 │ counter   │ Total number of HTTP requests             │ {'method':'post','code':'200'} │     1027 │ 1395066363000 │
│ http_requests_total                 │ counter   │                                           │ {'method':'post','code':'400'} │        3 │ 1395066363000 │
│ metric_without_timestamp_and_labels │           │                                           │ {}                             │    12.47 │             0 │
│ rpc_duration_seconds                │ summary   │ A summary of the RPC duration in seconds. │ {'quantile':'0.01'}            │     3102 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'quantile':'0.05'}            │     3272 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'quantile':'0.5'}             │     4773 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'quantile':'0.9'}             │     9001 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'quantile':'0.99'}            │    76656 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'count':''}                   │     2693 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'sum':''}                     │ 17560473 │             0 │
│ something_weird                     │           │                                           │ {'problem':'division by zero'} │      inf │      -3982045 │
└─────────────────────────────────────┴───────────┴───────────────────────────────────────────┴────────────────────────────────┴──────────┴───────────────┘
```

Will be formatted as:

```
# HELP http_request_duration_seconds A histogram of the request duration.
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.05"} 24054
http_request_duration_seconds_bucket{le="0.1"} 33444
http_request_duration_seconds_bucket{le="0.5"} 129389
http_request_duration_seconds_bucket{le="1"} 133988
http_request_duration_seconds_bucket{le="+Inf"} 144320
http_request_duration_seconds_sum 53423
http_request_duration_seconds_count 144320

# HELP http_requests_total Total number of HTTP requests
# TYPE http_requests_total counter
http_requests_total{code="200",method="post"} 1027 1395066363000
http_requests_total{code="400",method="post"} 3 1395066363000

metric_without_timestamp_and_labels 12.47

# HELP rpc_duration_seconds A summary of the RPC duration in seconds.
# TYPE rpc_duration_seconds summary
rpc_duration_seconds{quantile="0.01"} 3102
rpc_duration_seconds{quantile="0.05"} 3272
rpc_duration_seconds{quantile="0.5"} 4773
rpc_duration_seconds{quantile="0.9"} 9001
rpc_duration_seconds{quantile="0.99"} 76656
rpc_duration_seconds_sum 17560473
rpc_duration_seconds_count 2693

something_weird{problem="division by zero"} +Inf -3982045
```

## Protobuf {#protobuf}

Protobuf - is a [Protocol Buffers](https://protobuf.dev/) format.

This format requires an external format schema. The schema is cached between queries.
ClickHouse supports both `proto2` and `proto3` syntaxes. Repeated/optional/required fields are supported.

Usage examples:

``` sql
SELECT * FROM test.table FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

``` bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.table SETTINGS format_schema='schemafile:MessageType' FORMAT Protobuf"
```

where the file `schemafile.proto` looks like this:

``` capnp
syntax = "proto3";

message MessageType {
  string name = 1;
  string surname = 2;
  uint32 birthDate = 3;
  repeated string phoneNumbers = 4;
};
```

To find the correspondence between table columns and fields of Protocol Buffers’ message type ClickHouse compares their names.
This comparison is case-insensitive and the characters `_` (underscore) and `.` (dot) are considered as equal.
If types of a column and a field of Protocol Buffers’ message are different the necessary conversion is applied.

Nested messages are supported. For example, for the field `z` in the following message type

``` capnp
message MessageType {
  message XType {
    message YType {
      int32 z;
    };
    repeated YType y;
  };
  XType x;
};
```

ClickHouse tries to find a column named `x.y.z` (or `x_y_z` or `X.y_Z` and so on).
Nested messages are suitable to input or output a [nested data structures](/docs/en/sql-reference/data-types/nested-data-structures/index.md).

Default values defined in a protobuf schema like this

``` capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

are not applied; the [table defaults](/docs/en/sql-reference/statements/create/table.md/#create-default-values) are used instead of them.

ClickHouse inputs and outputs protobuf messages in the `length-delimited` format.
It means before every message should be written its length as a [varint](https://developers.google.com/protocol-buffers/docs/encoding#varints).
See also [how to read/write length-delimited protobuf messages in popular languages](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

### Using autogenerated schema {#using-autogenerated-protobuf-schema}

If you don't have an external Protobuf schema for your data, you can still output/input data in Protobuf format using autogenerated schema.
For example:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1
```

In this case ClickHouse will autogenerate Protobuf schema according to the table structure using function [structureToProtobufSchema](../sql-reference/functions/other-functions.md#structure_to_protobuf_schema) and will use this schema to serialize data in Protobuf format.

You can also read Protobuf file with autogenerated schema (in this case the file must be created using the same schema):

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_protobuf_use_autogenerated_schema=1 FORMAT Protobuf"
```

The setting [format_protobuf_use_autogenerated_schema](../operations/settings/settings-formats.md#format_protobuf_use_autogenerated_schema) is enabled by default and applies if [format_schema](../operations/settings/settings-formats.md#formatschema-format-schema) is not set.

You can also save autogenerated schema in the file during input/output using setting [output_format_schema](../operations/settings/settings-formats.md#outputformatschema-output-format-schema). For example:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1, output_format_schema='path/to/schema/schema.proto'
```

In this case autogenerated Protobuf schema will be saved in file `path/to/schema/schema.capnp`.

### Drop Protobuf cache

To reload Protobuf schema loaded from [format_schema_path](../operations/server-configuration-parameters/settings.md/#format_schema_path) use [SYSTEM DROP ... FORMAT CACHE](../sql-reference/statements/system.md/#system-drop-schema-format) statement.

```sql
SYSTEM DROP FORMAT SCHEMA CACHE FOR Protobuf
```

## ProtobufSingle {#protobufsingle}

Same as [Protobuf](#protobuf) but for storing/parsing single Protobuf message without length delimiters.

## ProtobufList {#protobuflist}

Similar to Protobuf but rows are represented as a sequence of sub-messages contained in a message with fixed name "Envelope".

Usage example:

``` sql
SELECT * FROM test.table FORMAT ProtobufList SETTINGS format_schema = 'schemafile:MessageType'
```

``` bash
cat protobuflist_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT ProtobufList SETTINGS format_schema='schemafile:MessageType'"
```

where the file `schemafile.proto` looks like this:

``` capnp
syntax = "proto3";
message Envelope {
  message MessageType {
    string name = 1;
    string surname = 2;
    uint32 birthDate = 3;
    repeated string phoneNumbers = 4;
  };
  MessageType row = 1;
};
```

## Avro {#data-format-avro}

[Apache Avro](https://avro.apache.org/) is a row-oriented data serialization framework developed within Apache’s Hadoop project.

ClickHouse Avro format supports reading and writing [Avro data files](https://avro.apache.org/docs/current/spec.html#Object+Container+Files).

### Data Types Matching {#data_types-matching}

The table below shows supported data types and how they match ClickHouse [data types](/docs/en/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| Avro data type `INSERT`                     | ClickHouse data type                                                                                                          | Avro data type `SELECT`       |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|-------------------------------|
| `boolean`, `int`, `long`, `float`, `double` | [Int(8\16\32)](/docs/en/sql-reference/data-types/int-uint.md), [UInt(8\16\32)](/docs/en/sql-reference/data-types/int-uint.md) | `int`                         |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](/docs/en/sql-reference/data-types/int-uint.md), [UInt64](/docs/en/sql-reference/data-types/int-uint.md)               | `long`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](/docs/en/sql-reference/data-types/float.md)                                                                         | `float`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](/docs/en/sql-reference/data-types/float.md)                                                                         | `double`                      |
| `bytes`, `string`, `fixed`, `enum`          | [String](/docs/en/sql-reference/data-types/string.md)                                                                         | `bytes` or `string` \*        |
| `bytes`, `string`, `fixed`                  | [FixedString(N)](/docs/en/sql-reference/data-types/fixedstring.md)                                                            | `fixed(N)`                    |
| `enum`                                      | [Enum(8\16)](/docs/en/sql-reference/data-types/enum.md)                                                                       | `enum`                        |
| `array(T)`                                  | [Array(T)](/docs/en/sql-reference/data-types/array.md)                                                                        | `array(T)`                    |
| `map(V, K)`                                 | [Map(V, K)](/docs/en/sql-reference/data-types/map.md)                                                                         | `map(string, K)`              |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](/docs/en/sql-reference/data-types/date.md)                                                                      | `union(null, T)`              |
| `null`                                      | [Nullable(Nothing)](/docs/en/sql-reference/data-types/special-data-types/nothing.md)                                          | `null`                        |
| `int (date)` \**                            | [Date](/docs/en/sql-reference/data-types/date.md), [Date32](docs/en/sql-reference/data-types/date32.md)                       | `int (date)` \**              |
| `long (timestamp-millis)` \**               | [DateTime64(3)](/docs/en/sql-reference/data-types/datetime.md)                                                                | `long (timestamp-millis)` \** |
| `long (timestamp-micros)` \**               | [DateTime64(6)](/docs/en/sql-reference/data-types/datetime.md)                                                                | `long (timestamp-micros)` \** |
| `bytes (decimal)`  \**                      | [DateTime64(N)](/docs/en/sql-reference/data-types/datetime.md)                                                                | `bytes (decimal)`  \**        |
| `int`                                       | [IPv4](/docs/en/sql-reference/data-types/ipv4.md)                                                                             | `int`                         |
| `fixed(16)`                                 | [IPv6](/docs/en/sql-reference/data-types/ipv6.md)                                                                             | `fixed(16)`                   |
| `bytes (decimal)` \**                       | [Decimal(P, S)](/docs/en/sql-reference/data-types/decimal.md)                                                                 | `bytes (decimal)` \**         |
| `string (uuid)` \**                         | [UUID](/docs/en/sql-reference/data-types/uuid.md)                                                                             | `string (uuid)` \**           |
| `fixed(16)`                                 | [Int128/UInt128](/docs/en/sql-reference/data-types/int-uint.md)                                                               | `fixed(16)`                   |
| `fixed(32)`                                 | [Int256/UInt256](/docs/en/sql-reference/data-types/int-uint.md)                                                               | `fixed(32)`                   |
| `record`                                    | [Tuple](/docs/en/sql-reference/data-types/tuple.md)                                                                           | `record`                      |



\* `bytes` is default, controlled by [output_format_avro_string_column_pattern](/docs/en/operations/settings/settings-formats.md/#output_format_avro_string_column_pattern)
\** [Avro logical types](https://avro.apache.org/docs/current/spec.html#Logical+Types)

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

## ParquetMetadata {data-format-parquet-metadata}

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

[Apache Arrow](https://arrow.apache.org/) comes with two built-in columnar storage formats. ClickHouse supports read and write operations for these formats.

`Arrow` is Apache Arrow’s "file mode" format. It is designed for in-memory random access.

### Data Types Matching {#data-types-matching-arrow}

The table below shows supported data types and how they match ClickHouse [data types](/docs/en/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| Arrow data type (`INSERT`)              | ClickHouse data type                                                                                       | Arrow data type (`SELECT`) |
|-----------------------------------------|------------------------------------------------------------------------------------------------------------|----------------------------|
| `BOOL`                                  | [Bool](/docs/en/sql-reference/data-types/boolean.md)                                                       | `BOOL`                     |
| `UINT8`, `BOOL`                         | [UInt8](/docs/en/sql-reference/data-types/int-uint.md)                                                     | `UINT8`                    |
| `INT8`                                  | [Int8](/docs/en/sql-reference/data-types/int-uint.md)/[Enum8](/docs/en/sql-reference/data-types/enum.md)   | `INT8`                     |
| `UINT16`                                | [UInt16](/docs/en/sql-reference/data-types/int-uint.md)                                                    | `UINT16`                   |
| `INT16`                                 | [Int16](/docs/en/sql-reference/data-types/int-uint.md)/[Enum16](/docs/en/sql-reference/data-types/enum.md) | `INT16`                    |
| `UINT32`                                | [UInt32](/docs/en/sql-reference/data-types/int-uint.md)                                                    | `UINT32`                   |
| `INT32`                                 | [Int32](/docs/en/sql-reference/data-types/int-uint.md)                                                     | `INT32`                    |
| `UINT64`                                | [UInt64](/docs/en/sql-reference/data-types/int-uint.md)                                                    | `UINT64`                   |
| `INT64`                                 | [Int64](/docs/en/sql-reference/data-types/int-uint.md)                                                     | `INT64`                    |
| `FLOAT`, `HALF_FLOAT`                   | [Float32](/docs/en/sql-reference/data-types/float.md)                                                      | `FLOAT32`                  |
| `DOUBLE`                                | [Float64](/docs/en/sql-reference/data-types/float.md)                                                      | `FLOAT64`                  |
| `DATE32`                                | [Date32](/docs/en/sql-reference/data-types/date32.md)                                                      | `UINT16`                   |
| `DATE64`                                | [DateTime](/docs/en/sql-reference/data-types/datetime.md)                                                  | `UINT32`                   |
| `TIMESTAMP`, `TIME32`, `TIME64`         | [DateTime64](/docs/en/sql-reference/data-types/datetime64.md)                                              | `UINT32`                   |
| `STRING`, `BINARY`                      | [String](/docs/en/sql-reference/data-types/string.md)                                                      | `BINARY`                   |
| `STRING`, `BINARY`, `FIXED_SIZE_BINARY` | [FixedString](/docs/en/sql-reference/data-types/fixedstring.md)                                            | `FIXED_SIZE_BINARY`        |
| `DECIMAL`                               | [Decimal](/docs/en/sql-reference/data-types/decimal.md)                                                    | `DECIMAL`                  |
| `DECIMAL256`                            | [Decimal256](/docs/en/sql-reference/data-types/decimal.md)                                                 | `DECIMAL256`               |
| `LIST`                                  | [Array](/docs/en/sql-reference/data-types/array.md)                                                        | `LIST`                     |
| `STRUCT`                                | [Tuple](/docs/en/sql-reference/data-types/tuple.md)                                                        | `STRUCT`                   |
| `MAP`                                   | [Map](/docs/en/sql-reference/data-types/map.md)                                                            | `MAP`                      |
| `UINT32`                                | [IPv4](/docs/en/sql-reference/data-types/ipv4.md)                                                          | `UINT32`                   |
| `FIXED_SIZE_BINARY`, `BINARY`           | [IPv6](/docs/en/sql-reference/data-types/ipv6.md)                                                          | `FIXED_SIZE_BINARY`        |
| `FIXED_SIZE_BINARY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/docs/en/sql-reference/data-types/int-uint.md)                             | `FIXED_SIZE_BINARY`        |

Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types also can be nested.

The `DICTIONARY` type is supported for `INSERT` queries, and for `SELECT` queries there is an [output_format_arrow_low_cardinality_as_dictionary](/docs/en/operations/settings/settings-formats.md/#output-format-arrow-low-cardinality-as-dictionary) setting that allows to output [LowCardinality](/docs/en/sql-reference/data-types/lowcardinality.md) type as a `DICTIONARY` type.

Unsupported Arrow data types: `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

The data types of ClickHouse table columns do not have to match the corresponding Arrow data fields. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/docs/en/sql-reference/functions/type-conversion-functions.md/#type_conversion_function-cast) the data to the data type set for the ClickHouse table column.

### Inserting Data {#inserting-data-arrow}

You can insert Arrow data from a file into ClickHouse table by the following command:

``` bash
$ cat filename.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

### Selecting Data {#selecting-data-arrow}

You can select data from a ClickHouse table and save them into some file in the Arrow format by the following command:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Arrow" > {filename.arrow}
```

### Arrow format settings {#arrow-format-settings}

- [output_format_arrow_low_cardinality_as_dictionary](/docs/en/operations/settings/settings-formats.md/#output_format_arrow_low_cardinality_as_dictionary) - enable output ClickHouse LowCardinality type as Dictionary Arrow type. Default value - `false`.
- [output_format_arrow_use_64_bit_indexes_for_dictionary](/docs/en/operations/settings/settings-formats.md/#output_format_arrow_use_64_bit_indexes_for_dictionary) - use 64-bit integer type for Dictionary indexes. Default value - `false`.
- [output_format_arrow_use_signed_indexes_for_dictionary](/docs/en/operations/settings/settings-formats.md/#output_format_arrow_use_signed_indexes_for_dictionary) - use signed integer type for Dictionary indexes. Default value - `true`.
- [output_format_arrow_string_as_string](/docs/en/operations/settings/settings-formats.md/#output_format_arrow_string_as_string) - use Arrow String type instead of Binary for String columns. Default value - `false`.
- [input_format_arrow_case_insensitive_column_matching](/docs/en/operations/settings/settings-formats.md/#input_format_arrow_case_insensitive_column_matching) - ignore case when matching Arrow columns with ClickHouse columns. Default value - `false`.
- [input_format_arrow_allow_missing_columns](/docs/en/operations/settings/settings-formats.md/#input_format_arrow_allow_missing_columns) - allow missing columns while reading Arrow data. Default value - `false`.
- [input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference](/docs/en/operations/settings/settings-formats.md/#input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference) - allow skipping columns with unsupported types while schema inference for Arrow format. Default value - `false`.
- [output_format_arrow_fixed_string_as_fixed_byte_array](/docs/en/operations/settings/settings-formats.md/#output_format_arrow_fixed_string_as_fixed_byte_array) - use Arrow FIXED_SIZE_BINARY type instead of Binary/String for FixedString columns. Default value - `true`.
- [output_format_arrow_compression_method](/docs/en/operations/settings/settings-formats.md/#output_format_arrow_compression_method) - compression method used in output Arrow format. Default value - `lz4_frame`.

## ArrowStream {#data-format-arrow-stream}

`ArrowStream` is Apache Arrow’s “stream mode” format. It is designed for in-memory stream processing.

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

If you input or output data via the [client](/docs/en/interfaces/cli.md) in the [interactive mode](/docs/en/interfaces/cli.md/#cli_usage), the file name specified in the format schema
can contain an absolute path or a path relative to the current directory on the client.
If you use the client in the [batch mode](/docs/en/interfaces/cli.md/#cli_usage), the path to the schema must be relative due to security reasons.

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
