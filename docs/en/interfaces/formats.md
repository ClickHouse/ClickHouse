---
toc_priority: 21
toc_title: Input and Output Formats
---

# Formats for Input and Output Data {#formats}

ClickHouse can accept and return data in various formats. A format supported for input can be used to parse the data provided to `INSERT`s, to perform `SELECT`s from a file-backed table such as File, URL or HDFS, or to read an external dictionary. A format supported for output can be used to arrange the
results of a `SELECT`, and to perform `INSERT`s into a file-backed table.

The supported formats are:

| Format                                                                                  | Input | Output |
|-----------------------------------------------------------------------------------------|-------|--------|
| [TabSeparated](#tabseparated)                                                           | ✔     | ✔      |
| [TabSeparatedRaw](#tabseparatedraw)                                                     | ✔     | ✔      |
| [TabSeparatedWithNames](#tabseparatedwithnames)                                         | ✔     | ✔      |
| [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes)                         | ✔     | ✔      |
| [TabSeparatedRawWithNames](#tabseparatedrawwithnames)                                                     | ✔     | ✔      |
| [TabSeparatedRawWithNamesAndTypes](#tabseparatedrawwithnamesandtypes)                                                     | ✔     | ✔      |
| [Template](#format-template)                                                            | ✔     | ✔      |
| [TemplateIgnoreSpaces](#templateignorespaces)                                           | ✔     | ✗      |
| [CSV](#csv)                                                                             | ✔     | ✔      |
| [CSVWithNames](#csvwithnames)                                                           | ✔     | ✔      |
| [CSVWithNamesAndTypes](#csvwithnamesandtypes)                                                           | ✔     | ✔      |
| [CustomSeparated](#format-customseparated)                                              | ✔     | ✔      |
| [Values](#data-format-values)                                                           | ✔     | ✔      |
| [Vertical](#vertical)                                                                   | ✗     | ✔      |
| [JSON](#json)                                                                           | ✗     | ✔      |
| [JSONAsString](#jsonasstring)                                                           | ✔     | ✗      |
| [JSONStrings](#jsonstrings)                                                               | ✗     | ✔      |
| [JSONCompact](#jsoncompact)                                                             | ✗     | ✔      |
| [JSONCompactStrings](#jsoncompactstrings)                                                 | ✗     | ✔      |
| [JSONEachRow](#jsoneachrow)                                                             | ✔     | ✔      |
| [JSONEachRowWithProgress](#jsoneachrowwithprogress)                                     | ✗     | ✔      |
| [JSONStringsEachRow](#jsonstringseachrow)                                               | ✔     | ✔      |
| [JSONStringsEachRowWithProgress](#jsonstringseachrowwithprogress)                       | ✗     | ✔      |
| [JSONCompactEachRow](#jsoncompacteachrow)                                               | ✔     | ✔      |
| [JSONCompactEachRowWithNames](#jsoncompacteachrowwithnames)             | ✔     | ✔      |
| [JSONCompactEachRowWithNamesAndTypes](#jsoncompacteachrowwithnamesandtypes)             | ✔     | ✔      |
| [JSONCompactStringsEachRow](#jsoncompactstringseachrow)                                   | ✔     | ✔      |
| [JSONCompactStringsEachRowWithNames](#jsoncompactstringseachrowwithnames) | ✔     | ✔      |
| [JSONCompactStringsEachRowWithNamesAndTypes](#jsoncompactstringseachrowwithnamesandtypes) | ✔     | ✔      |
| [TSKV](#tskv)                                                                           | ✔     | ✔      |
| [Pretty](#pretty)                                                                       | ✗     | ✔      |
| [PrettyCompact](#prettycompact)                                                         | ✗     | ✔      |
| [PrettyCompactMonoBlock](#prettycompactmonoblock)                                       | ✗     | ✔      |
| [PrettyNoEscapes](#prettynoescapes)                                                     | ✗     | ✔      |
| [PrettySpace](#prettyspace)                                                             | ✗     | ✔      |
| [Protobuf](#protobuf)                                                                   | ✔     | ✔      |
| [ProtobufSingle](#protobufsingle)                                                       | ✔     | ✔      |
| [Avro](#data-format-avro)                                                               | ✔     | ✔      |
| [AvroConfluent](#data-format-avro-confluent)                                            | ✔     | ✗      |
| [Parquet](#data-format-parquet)                                                         | ✔     | ✔      |
| [Arrow](#data-format-arrow)                                                             | ✔     | ✔      |
| [ArrowStream](#data-format-arrow-stream)                                                | ✔     | ✔      |
| [ORC](#data-format-orc)                                                                 | ✔     | ✔      |
| [RowBinary](#rowbinary)                                                                 | ✔     | ✔      |
| [RowBinaryWithNames](#rowbinarywithnamesandtypes)                               | ✔     | ✔      |
| [RowBinaryWithNamesAndTypes](#rowbinarywithnamesandtypes)                               | ✔     | ✔      |
| [Native](#native)                                                                       | ✔     | ✔      |
| [Null](#null)                                                                           | ✗     | ✔      |
| [XML](#xml)                                                                             | ✗     | ✔      |
| [CapnProto](#capnproto)                                                                 | ✔     | ✗      |
| [LineAsString](#lineasstring)                                                           | ✔     | ✗      |
| [Regexp](#data-format-regexp)                                                           | ✔     | ✗      |
| [RawBLOB](#rawblob)                                                                     | ✔     | ✔      |
| [MsgPack](#msgpack)                                                                     | ✔     | ✔      |

You can control some format processing parameters with the ClickHouse settings. For more information read the [Settings](../operations/settings/settings.md) section.

## TabSeparated {#tabseparated}

In TabSeparated format, data is written by row. Each row contains values separated by tabs. Each value is followed by a tab, except the last value in the row, which is followed by a line feed. Strictly Unix line feeds are assumed everywhere. The last row also must contain a line feed at the end. Values are written in text format, without enclosing quotation marks, and with special characters escaped.

This format is also available under the name `TSV`.

The `TabSeparated` format is convenient for processing data using custom programs and scripts. It is used by default in the HTTP interface, and in the command-line client’s batch mode. This format also allows transferring data between different DBMSs. For example, you can get a dump from MySQL and upload it to ClickHouse, or vice versa.

The `TabSeparated` format supports outputting total values (when using WITH TOTALS) and extreme values (when ‘extremes’ is set to 1). In these cases, the total values and extremes are output after the main data. The main result, total values, and extremes are separated from each other by an empty line. Example:

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated
```

``` text
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

### Data Formatting {#data-formatting}

Integer numbers are written in decimal form. Numbers can contain an extra “+” character at the beginning (ignored when parsing, and not recorded when formatting). Non-negative numbers can’t contain the negative sign. When reading, it is allowed to parse an empty string as a zero, or (for signed types) a string consisting of just a minus sign as a zero. Numbers that do not fit into the corresponding data type may be parsed as a different number, without an error message.

Floating-point numbers are written in decimal form. The dot is used as the decimal separator. Exponential entries are supported, as are ‘inf’, ‘+inf’, ‘-inf’, and ‘nan’. An entry of floating-point numbers may begin or end with a decimal point.
During formatting, accuracy may be lost on floating-point numbers.
During parsing, it is not strictly required to read the nearest machine-representable number.

Dates are written in YYYY-MM-DD format and parsed in the same format, but with any characters as separators.
Dates with times are written in the format `YYYY-MM-DD hh:mm:ss` and parsed in the same format, but with any characters as separators.
This all occurs in the system time zone at the time the client or server starts (depending on which of them formats data). For dates with times, daylight saving time is not specified. So if a dump has times during daylight saving time, the dump does not unequivocally match the data, and parsing will select one of the two times.
During a read operation, incorrect dates and dates with times can be parsed with natural overflow or as null dates and times, without an error message.

As an exception, parsing dates with times is also supported in Unix timestamp format, if it consists of exactly 10 decimal digits. The result is not time zone-dependent. The formats YYYY-MM-DD hh:mm:ss and NNNNNNNNNN are differentiated automatically.

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

[NULL](../sql-reference/syntax.md) is formatted according to setting [format_tsv_null_representation](../operations/settings/settings.md#settings-format_tsv_null_representation) (default value is `\N`).


If setting [input_format_tsv_empty_as_default](../operations/settings/settings.md#settings-input_format_tsv_empty_as_default) is enabled,
empty input fields are replaced with default values. For complex default expressions [input_format_defaults_for_omitted_fields](../operations/settings/settings.md#settings-input_format_defaults_for_omitted_fields) must be enabled too.

Each element of [Nested](../sql-reference/data-types/nested-data-structures/nested.md) structures is represented as array.

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

``` text
1  [1]    ['a']
```

## TabSeparatedRaw {#tabseparatedraw}

Differs from `TabSeparated` format in that the rows are written without escaping.
When parsing with this format, tabs or linefeeds are not allowed in each field.

This format is also available under the name `TSVRaw`.

## TabSeparatedWithNames {#tabseparatedwithnames}

Differs from the `TabSeparated` format in that the column names are written in the first row.
If setting [input_format_with_names_use_header](../operations/settings/settings.md#settings-input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](../operations/settings/settings.md#settings-input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.

This format is also available under the name `TSVWithNames`.

## TabSeparatedWithNamesAndTypes {#tabseparatedwithnamesandtypes}

Differs from the `TabSeparated` format in that the column names are written to the first row, while the column types are in the second row.
The first row with names is processed the same way as in `TabSeparatedWithNames` format.
If setting [input_format_with_types_use_header](../operations/settings/settings.md#settings-input_format_with_types_use_header) is set to 1,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped. 

This format is also available under the name `TSVWithNamesAndTypes`.

## TabSeparatedRawWithNames {#tabseparatedrawwithnames}

Differs from `TabSeparatedWithNames` format in that the rows are written without escaping.
When parsing with this format, tabs or linefeeds are not allowed in each field.

This format is also available under the name `TSVRawWithNames`.

## TabSeparatedWithNamesAndTypes {#tabseparatedrawwithnamesandtypes}

Differs from `TabSeparatedWithNamesAndTypes` format in that the rows are written without escaping.
When parsing with this format, tabs or linefeeds are not allowed in each field.

This format is also available under the name `TSVRawWithNamesAndNames`.

## Template {#format-template}

This format allows specifying a custom format string with placeholders for values with a specified escaping rule.

It uses settings `format_template_resultset`, `format_template_row`, `format_template_rows_between_delimiter` and some settings of other formats (e.g. `output_format_json_quote_64bit_integers` when using `JSON` escaping, see further)

Setting `format_template_row` specifies path to file, which contains format string for rows with the following syntax:

`delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N`,

where `delimiter_i` is a delimiter between values (`$` symbol can be escaped as `$$`),
`column_i` is a name or index of a column whose values are to be selected or inserted (if empty, then column will be skipped),
`serializeAs_i` is an escaping rule for the column values. The following escaping rules are supported:

-   `CSV`, `JSON`, `XML` (similarly to the formats of the same names)
-   `Escaped` (similarly to `TSV`)
-   `Quoted` (similarly to `Values`)
-   `Raw` (without escaping, similarly to `TSVRaw`)
-   `None` (no escaping rule, see further)

If an escaping rule is omitted, then `None` will be used. `XML` is suitable only for output.

So, for the following format string:

      `Search phrase: ${SearchPhrase:Quoted}, count: ${c:Escaped}, ad price: $$${price:JSON};`

the values of `SearchPhrase`, `c` and `price` columns, which are escaped as `Quoted`, `Escaped` and `JSON` will be printed (for select) or will be expected (for insert) between `Search phrase:`, `, count:`, `, ad price: $` and `;` delimiters respectively. For example:

`Search phrase: 'bathroom interior design', count: 2166, ad price: $3;`

The `format_template_rows_between_delimiter` setting specifies delimiter between rows, which is printed (or expected) after every row except the last one (`\n` by default)

Setting `format_template_resultset` specifies the path to file, which contains a format string for resultset. Format string for resultset has the same syntax as a format string for row and allows to specify a prefix, a suffix and a way to print some additional information. It contains the following placeholders instead of column names:

-   `data` is the rows with data in `format_template_row` format, separated by `format_template_rows_between_delimiter`. This placeholder must be the first placeholder in the format string.
-   `totals` is the row with total values in `format_template_row` format (when using WITH TOTALS)
-   `min` is the row with minimum values in `format_template_row` format (when extremes are set to 1)
-   `max` is the row with maximum values in `format_template_row` format (when extremes are set to 1)
-   `rows` is the total number of output rows
-   `rows_before_limit` is the minimal number of rows there would have been without LIMIT. Output only if the query contains LIMIT. If the query contains GROUP BY, rows_before_limit_at_least is the exact number of rows there would have been without a LIMIT.
-   `time` is the request execution time in seconds
-   `rows_read` is the number of rows has been read
-   `bytes_read` is the number of bytes (uncompressed) has been read

The placeholders `data`, `totals`, `min` and `max` must not have escaping rule specified (or `None` must be specified explicitly). The remaining placeholders may have any escaping rule specified.
If the `format_template_resultset` setting is an empty string, `${data}` is used as default value.
For insert queries format allows skipping some columns or some fields if prefix or suffix (see example).

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
    <tr> <td>yandex</td> <td>1655</td> </tr>
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
INSERT INTO UserActivity FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
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
Similar to `Template`, but skips whitespace characters between delimiters and values in the input stream. However, if format strings contain whitespace characters, these characters will be expected in the input stream. Also allows to specify empty placeholders (`${}` or `${:None}`) to split some delimiter into separate parts to ignore spaces between them. Such placeholders are used only for skipping whitespace characters.
It’s possible to read `JSON` using this format, if values of columns have the same order in all rows. For example, the following request can be used for inserting data from output example of format [JSON](#json):

``` sql
INSERT INTO table_name FORMAT TemplateIgnoreSpaces SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = ','
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
SearchPhrase=yandex     count()=1655
SearchPhrase=2014 spring fashion    count()=1549
SearchPhrase=freeform photos       count()=1480
SearchPhrase=angelina jolie    count()=1245
SearchPhrase=omsk       count()=1112
SearchPhrase=photos of dog breeds    count()=1091
SearchPhrase=curtain designs        count()=1064
SearchPhrase=baku       count()=1000
```

[NULL](../sql-reference/syntax.md) is formatted as `\N`.

``` sql
SELECT * FROM t_null FORMAT TSKV
```

``` text
x=1    y=\N
```

When there is a large number of small columns, this format is ineffective, and there is generally no reason to use it. Nevertheless, it is no worse than JSONEachRow in terms of efficiency.

Both data output and parsing are supported in this format. For parsing, any order is supported for the values of different columns. It is acceptable for some values to be omitted – they are treated as equal to their default values. In this case, zeros and blank rows are used as default values. Complex values that could be specified in the table are not supported as defaults.

Parsing allows the presence of the additional field `tskv` without the equal sign or a value. This field is ignored.

## CSV {#csv}

Comma Separated Values format ([RFC](https://tools.ietf.org/html/rfc4180)).

When formatting, rows are enclosed in double-quotes. A double quote inside a string is output as two double quotes in a row. There are no other rules for escaping characters. Date and date-time are enclosed in double-quotes. Numbers are output without quotes. Values are separated by a delimiter character, which is `,` by default. The delimiter character is defined in the setting [format_csv_delimiter](../operations/settings/settings.md#settings-format_csv_delimiter). Rows are separated using the Unix line feed (LF). Arrays are serialized in CSV as follows: first, the array is serialized to a string as in TabSeparated format, and then the resulting string is output to CSV in double-quotes. Tuples in CSV format are serialized as separate columns (that is, their nesting in the tuple is lost).

``` bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

\*By default, the delimiter is `,`. See the [format_csv_delimiter](../operations/settings/settings.md#settings-format_csv_delimiter) setting for more information.

When parsing, all values can be parsed either with or without quotes. Both double and single quotes are supported. Rows can also be arranged without quotes. In this case, they are parsed up to the delimiter character or line feed (CR or LF). In violation of the RFC, when parsing rows without quotes, the leading and trailing spaces and tabs are ignored. For the line feed, Unix (LF), Windows (CR LF) and Mac OS Classic (CR LF) types are all supported.

If setting [input_format_csv_empty_as_default](../operations/settings/settings.md#settings-input_format_csv_empty_as_default) is enabled,
empty unquoted input values are replaced with default values. For complex default expressions [input_format_defaults_for_omitted_fields](../operations/settings/settings.md#settings-input_format_defaults_for_omitted_fields) must be enabled too.

`NULL` is formatted according to setting [format_csv_null_representation](../operations/settings/settings.md#settings-format_csv_null_representation) (default value is `\N`).

The CSV format supports the output of totals and extremes the same way as `TabSeparated`.

## CSVWithNames {#csvwithnames}

Also prints the header row with column names, similar to [TabSeparatedWithNames](#tabseparatedwithnames).

## CSVWithNamesAndTypes {#csvwithnamesandtypes}

Also prints two header rows with column names and types, similar to [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes).

## CustomSeparated {#format-customseparated}

Similar to [Template](#format-template), but it prints or reads all columns and uses escaping rule from setting `format_custom_escaping_rule` and delimiters from settings `format_custom_field_delimiter`, `format_custom_row_before_delimiter`, `format_custom_row_after_delimiter`, `format_custom_row_between_delimiter`, `format_custom_result_before_delimiter` and `format_custom_result_after_delimiter`, not from format strings.
There is also `CustomSeparatedIgnoreSpaces` format, which is similar to `TemplateIgnoreSpaces`.

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
                        "name": "'hello'",
                        "type": "String"
                },
                {
                        "name": "multiply(42, number)",
                        "type": "UInt64"
                },
                {
                        "name": "range(5)",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        [
                {
                        "'hello'": "hello",
                        "multiply(42, number)": "0",
                        "range(5)": [0,1,2,3,4]
                },
                {
                        "'hello'": "hello",
                        "multiply(42, number)": "42",
                        "range(5)": [0,1,2,3,4]
                },
                {
                        "'hello'": "hello",
                        "multiply(42, number)": "84",
                        "range(5)": [0,1,2,3,4]
                }
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3
}
```

The JSON is compatible with JavaScript. To ensure this, some characters are additionally escaped: the slash `/` is escaped as `\/`; alternative line breaks `U+2028` and `U+2029`, which break some browsers, are escaped as `\uXXXX`. ASCII control characters are escaped: backspace, form feed, line feed, carriage return, and horizontal tab are replaced with `\b`, `\f`, `\n`, `\r`, `\t` , as well as the remaining bytes in the 00-1F range using `\uXXXX` sequences. Invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences. For compatibility with JavaScript, Int64 and UInt64 integers are enclosed in double-quotes by default. To remove the quotes, you can set the configuration parameter [output_format_json_quote_64bit_integers](../operations/settings/settings.md#session_settings-output_format_json_quote_64bit_integers) to 0.

`rows` – The total number of output rows.

`rows_before_limit_at_least` The minimal number of rows there would have been without LIMIT. Output only if the query contains LIMIT.
If the query contains GROUP BY, rows_before_limit_at_least is the exact number of rows there would have been without a LIMIT.

`totals` – Total values (when using WITH TOTALS).

`extremes` – Extreme values (when extremes are set to 1).

This format is only appropriate for outputting a query result, but not for parsing (retrieving data to insert in a table).

ClickHouse supports [NULL](../sql-reference/syntax.md), which is displayed as `null` in the JSON output. To enable `+nan`, `-nan`, `+inf`, `-inf` values in output, set the [output_format_json_quote_denormals](../operations/settings/settings.md#settings-output_format_json_quote_denormals) to 1.

**See Also**

-   [JSONEachRow](#jsoneachrow) format
-   [output_format_json_array_of_rows](../operations/settings/settings.md#output-format-json-array-of-rows) setting

## JSONStrings {#jsonstrings}

Differs from JSON only in that data fields are output in strings, not in typed JSON values.

Example:

```json
{
        "meta":
        [
                {
                        "name": "'hello'",
                        "type": "String"
                },
                {
                        "name": "multiply(42, number)",
                        "type": "UInt64"
                },
                {
                        "name": "range(5)",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        [
                {
                        "'hello'": "hello",
                        "multiply(42, number)": "0",
                        "range(5)": "[0,1,2,3,4]"
                },
                {
                        "'hello'": "hello",
                        "multiply(42, number)": "42",
                        "range(5)": "[0,1,2,3,4]"
                },
                {
                        "'hello'": "hello",
                        "multiply(42, number)": "84",
                        "range(5)": "[0,1,2,3,4]"
                }
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3
}
```

## JSONAsString {#jsonasstring}

In this format, a single JSON object is interpreted as a single value. If the input has several JSON objects (comma separated), they are interpreted as separate rows. If the input data is enclosed in square brackets, it is interpreted as an array of JSONs.

This format can only be parsed for table with a single field of type [String](../sql-reference/data-types/string.md). The remaining columns must be set to [DEFAULT](../sql-reference/statements/create/table.md#default) or [MATERIALIZED](../sql-reference/statements/create/table.md#materialized), or omitted. Once you collect whole JSON object to string you can use [JSON functions](../sql-reference/functions/json-functions.md) to process it.

**Examples**

Query:

``` sql
DROP TABLE IF EXISTS json_as_string;
CREATE TABLE json_as_string (json String) ENGINE = Memory;
INSERT INTO json_as_string (json) FORMAT JSONAsString {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_string;
```

Result:

``` text
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

```text
┌─field──────────────────────┐
│ {"id": 1, "name": "name1"} │
│ {"id": 2, "name": "name2"} │
└────────────────────────────┘
```

## JSONCompact {#jsoncompact}
## JSONCompactStrings {#jsoncompactstrings}

Differs from JSON only in that data rows are output in arrays, not in objects.

Example:

```
// JSONCompact
{
        "meta":
        [
                {
                        "name": "'hello'",
                        "type": "String"
                },
                {
                        "name": "multiply(42, number)",
                        "type": "UInt64"
                },
                {
                        "name": "range(5)",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        [
                ["hello", "0", [0,1,2,3,4]],
                ["hello", "42", [0,1,2,3,4]],
                ["hello", "84", [0,1,2,3,4]]
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3
}
```

```
// JSONCompactStrings
{
        "meta":
        [
                {
                        "name": "'hello'",
                        "type": "String"
                },
                {
                        "name": "multiply(42, number)",
                        "type": "UInt64"
                },
                {
                        "name": "range(5)",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        [
                ["hello", "0", "[0,1,2,3,4]"],
                ["hello", "42", "[0,1,2,3,4]"],
                ["hello", "84", "[0,1,2,3,4]"]
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3
}
```

## JSONEachRow {#jsoneachrow}
## JSONStringsEachRow {#jsonstringseachrow}
## JSONCompactEachRow {#jsoncompacteachrow}
## JSONCompactStringsEachRow {#jsoncompactstringseachrow}

When using these formats, ClickHouse outputs rows as separated, newline-delimited JSON values, but the data as a whole is not valid JSON.

``` json
{"some_int":42,"some_str":"hello","some_tuple":[1,"a"]} // JSONEachRow
[42,"hello",[1,"a"]] // JSONCompactEachRow
["42","hello","(2,'a')"] // JSONCompactStringsEachRow
```

When inserting the data, you should provide a separate JSON value for each row.

## JSONEachRowWithProgress {#jsoneachrowwithprogress}
## JSONStringsEachRowWithProgress {#jsonstringseachrowwithprogress}

Differs from `JSONEachRow`/`JSONStringsEachRow` in that ClickHouse will also yield progress information as JSON values.

```json
{"row":{"'hello'":"hello","multiply(42, number)":"0","range(5)":[0,1,2,3,4]}}
{"row":{"'hello'":"hello","multiply(42, number)":"42","range(5)":[0,1,2,3,4]}}
{"row":{"'hello'":"hello","multiply(42, number)":"84","range(5)":[0,1,2,3,4]}}
{"progress":{"read_rows":"3","read_bytes":"24","written_rows":"0","written_bytes":"0","total_rows_to_read":"3"}}
```

## JSONCompactEachRowWithNames {#jsoncompacteachrowwithnames}

Differs from `JSONCompactEachRow` format in that it also prints the header row with column names, similar to [TabSeparatedWithNames](#tabseparatedwithnames).

## JSONCompactEachRowWithNamesAndTypes {#jsoncompacteachrowwithnamesandtypes}

Differs from `JSONCompactEachRow` format in that it also prints two header rows with column names and types, similar to [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes).

## JSONCompactStringsEachRowWithNames {#jsoncompactstringseachrowwithnames}

Differs from `JSONCompactStringsEachRow` in that in that it also prints the header row with column names, similar to [TabSeparatedWithNames](#tabseparatedwithnames).

## JSONCompactStringsEachRowWithNamesAndTypes {#jsoncompactstringseachrowwithnamesandtypes}

Differs from `JSONCompactStringsEachRow` in that it also prints two header rows with column names and types, similar to [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes).

```json
["'hello'", "multiply(42, number)", "range(5)"]
["String", "UInt64", "Array(UInt8)"]
["hello", "0", [0,1,2,3,4]]
["hello", "42", [0,1,2,3,4]]
["hello", "84", [0,1,2,3,4]]
```

### Inserting Data {#inserting-data}

``` sql
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

ClickHouse allows:

-   Any order of key-value pairs in the object.
-   Omitting some values.

ClickHouse ignores spaces between elements and commas after the objects. You can pass all the objects in one line. You do not have to separate them with line breaks.

**Omitted values processing**

ClickHouse substitutes omitted values with the default values for the corresponding [data types](../sql-reference/data-types/index.md).

If `DEFAULT expr` is specified, ClickHouse uses different substitution rules depending on the [input_format_defaults_for_omitted_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields) setting.

Consider the following table:

``` sql
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

-   If `input_format_defaults_for_omitted_fields = 0`, then the default value for `x` and `a` equals `0` (as the default value for the `UInt32` data type).
-   If `input_format_defaults_for_omitted_fields = 1`, then the default value for `x` equals `0`, but the default value of `a` equals `x * 2`.

!!! note "Warning"
    When inserting data with `input_format_defaults_for_omitted_fields = 1`, ClickHouse consumes more computational resources, compared to insertion with `input_format_defaults_for_omitted_fields = 0`.

### Selecting Data {#selecting-data}

Consider the `UserActivity` table as an example:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

The query `SELECT * FROM UserActivity FORMAT JSONEachRow` returns:

``` text
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

Unlike the [JSON](#json) format, there is no substitution of invalid UTF-8 sequences. Values are escaped in the same way as for `JSON`.

!!! note "Note"
    Any set of bytes can be output in the strings. Use the `JSONEachRow` format if you are sure that the data in the table can be formatted as JSON without losing any information.

### Usage of Nested Structures {#jsoneachrow-nested}

If you have a table with [Nested](../sql-reference/data-types/nested-data-structures/nested.md) data type columns, you can insert JSON data with the same structure. Enable this feature with the [input_format_import_nested_json](../operations/settings/settings.md#settings-input_format_import_nested_json) setting.

For example, consider the following table:

``` sql
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

As you can see in the `Nested` data type description, ClickHouse treats each component of the nested structure as a separate column (`n.s` and `n.i` for our table). You can insert data in the following way:

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

To insert data as a hierarchical JSON object, set [input_format_import_nested_json=1](../operations/settings/settings.md#settings-input_format_import_nested_json).

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

``` text
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

``` text
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

``` sql
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

``` text
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

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

[NULL](../sql-reference/syntax.md) is output as `ᴺᵁᴸᴸ`.

Example (shown for the [PrettyCompact](#prettycompact) format):

``` sql
SELECT * FROM t_null
```

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Rows are not escaped in Pretty\* formats. Example is shown for the [PrettyCompact](#prettycompact) format:

``` sql
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

``` text
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

``` text
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

## PrettyCompact {#prettycompact}

Differs from [Pretty](#pretty) in that the grid is drawn between rows and the result is more compact.
This format is used by default in the command-line client in interactive mode.

## PrettyCompactMonoBlock {#prettycompactmonoblock}

Differs from [PrettyCompact](#prettycompact) in that up to 10,000 rows are buffered, then output as a single table, not by blocks.

## PrettyNoEscapes {#prettynoescapes}

Differs from Pretty in that ANSI-escape sequences aren’t used. This is necessary for displaying this format in a browser, as well as for using the ‘watch’ command-line utility.

Example:

``` bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

You can use the HTTP interface for displaying in the browser.

### PrettyCompactNoEscapes {#prettycompactnoescapes}

The same as the previous setting.

### PrettySpaceNoEscapes {#prettyspacenoescapes}

The same as the previous setting.

## PrettySpace {#prettyspace}

Differs from [PrettyCompact](#prettycompact) in that whitespace (space characters) is used instead of the grid.

## RowBinary {#rowbinary}

Formats and parses data by row in binary format. Rows and values are listed consecutively, without separators.
This format is less efficient than the Native format since it is row-based.

Integers use fixed-length little-endian representation. For example, UInt64 uses 8 bytes.
DateTime is represented as UInt32 containing the Unix timestamp as the value.
Date is represented as a UInt16 object that contains the number of days since 1970-01-01 as the value.
String is represented as a varint length (unsigned [LEB128](https://en.wikipedia.org/wiki/LEB128)), followed by the bytes of the string.
FixedString is represented simply as a sequence of bytes.

Array is represented as a varint length (unsigned [LEB128](https://en.wikipedia.org/wiki/LEB128)), followed by successive elements of the array.

For [NULL](../sql-reference/syntax.md#null-literal) support, an additional byte containing 1 or 0 is added before each [Nullable](../sql-reference/data-types/nullable.md) value. If 1, then the value is `NULL` and this byte is interpreted as a separate value. If 0, the value after the byte is not `NULL`.

## RowBinaryWithNames {#rowbinarywithnames}

Similar to [RowBinary](#rowbinary), but with added header:

-   [LEB128](https://en.wikipedia.org/wiki/LEB128)-encoded number of columns (N)
-   N `String`s specifying column names

## RowBinaryWithNamesAndTypes {#rowbinarywithnamesandtypes}

Similar to [RowBinary](#rowbinary), but with added header:

-   [LEB128](https://en.wikipedia.org/wiki/LEB128)-encoded number of columns (N)
-   N `String`s specifying column names
-   N `String`s specifying column types

## Values {#data-format-values}

Prints every row in brackets. Rows are separated by commas. There is no comma after the last row. The values inside the brackets are also comma-separated. Numbers are output in a decimal format without quotes. Arrays are output in square brackets. Strings, dates, and dates with times are output in quotes. Escaping rules and parsing are similar to the [TabSeparated](#tabseparated) format. During formatting, extra spaces aren’t inserted, but during parsing, they are allowed and skipped (except for spaces inside array values, which are not allowed). [NULL](../sql-reference/syntax.md) is represented as `NULL`.

The minimum set of characters that you need to escape when passing data in Values ​​format: single quotes and backslashes.

This is the format that is used in `INSERT INTO t VALUES ...`, but you can also use it for formatting query results.

See also: [input_format_values_interpret_expressions](../operations/settings/settings.md#settings-input_format_values_interpret_expressions) and [input_format_values_deduce_templates_of_expressions](../operations/settings/settings.md#settings-input_format_values_deduce_templates_of_expressions) settings.

## Vertical {#vertical}

Prints each value on a separate line with the column name specified. This format is convenient for printing just one or a few rows if each row consists of a large number of columns.

[NULL](../sql-reference/syntax.md) is output as `ᴺᵁᴸᴸ`.

Example:

``` sql
SELECT * FROM t_null FORMAT Vertical
```

``` text
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

Rows are not escaped in Vertical format:

``` sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

``` text
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
                        <SearchPhrase>yandex</SearchPhrase>
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

Cap’n Proto is a binary message format similar to Protocol Buffers and Thrift, but not like JSON or MessagePack.

Cap’n Proto messages are strictly typed and not self-describing, meaning they need an external schema description. The schema is applied on the fly and cached for each query.

``` bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits FORMAT CapnProto SETTINGS format_schema='schema:Message'"
```

Where `schema.capnp` looks like this:

``` capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

Deserialization is effective and usually does not increase the system load.

See also [Format Schema](#formatschema).

## Protobuf {#protobuf}

Protobuf - is a [Protocol Buffers](https://developers.google.com/protocol-buffers/) format.

This format requires an external format schema. The schema is cached between queries.
ClickHouse supports both `proto2` and `proto3` syntaxes. Repeated/optional/required fields are supported.

Usage examples:

``` sql
SELECT * FROM test.table FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

``` bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT Protobuf SETTINGS format_schema='schemafile:MessageType'"
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
Nested messages are suitable to input or output a [nested data structures](../sql-reference/data-types/nested-data-structures/nested.md).

Default values defined in a protobuf schema like this

``` capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

are not applied; the [table defaults](../sql-reference/statements/create/table.md#create-default-values) are used instead of them.

ClickHouse inputs and outputs protobuf messages in the `length-delimited` format.
It means before every message should be written its length as a [varint](https://developers.google.com/protocol-buffers/docs/encoding#varints).
See also [how to read/write length-delimited protobuf messages in popular languages](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

## ProtobufSingle {#protobufsingle}

Same as [Protobuf](#protobuf) but for storing/parsing single Protobuf message without length delimiters.

## Avro {#data-format-avro}

[Apache Avro](https://avro.apache.org/) is a row-oriented data serialization framework developed within Apache’s Hadoop project.

ClickHouse Avro format supports reading and writing [Avro data files](https://avro.apache.org/docs/current/spec.html#Object+Container+Files).

### Data Types Matching {#data_types-matching}

The table below shows supported data types and how they match ClickHouse [data types](../sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| Avro data type `INSERT`                     | ClickHouse data type                                                                                                  | Avro data type `SELECT`      |
|---------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|------------------------------|
| `boolean`, `int`, `long`, `float`, `double` | [Int(8\|16\|32)](../sql-reference/data-types/int-uint.md), [UInt(8\|16\|32)](../sql-reference/data-types/int-uint.md) | `int`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](../sql-reference/data-types/int-uint.md), [UInt64](../sql-reference/data-types/int-uint.md)                   | `long`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](../sql-reference/data-types/float.md)                                                                       | `float`                      |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](../sql-reference/data-types/float.md)                                                                       | `double`                     |
| `bytes`, `string`, `fixed`, `enum`          | [String](../sql-reference/data-types/string.md)                                                                       | `bytes` or `string` \*       |
| `bytes`, `string`, `fixed`                  | [FixedString(N)](../sql-reference/data-types/fixedstring.md)                                                          | `fixed(N)`                   |
| `enum`                                      | [Enum(8\|16)](../sql-reference/data-types/enum.md)                                                                    | `enum`                       |
| `array(T)`                                  | [Array(T)](../sql-reference/data-types/array.md)                                                                      | `array(T)`                   |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](../sql-reference/data-types/date.md)                                                                    | `union(null, T)`             |
| `null`                                      | [Nullable(Nothing)](../sql-reference/data-types/special-data-types/nothing.md)                                        | `null`                       |
| `int (date)` \**                            | [Date](../sql-reference/data-types/date.md)                                                                           | `int (date)` \**             |
| `long (timestamp-millis)` \**               | [DateTime64(3)](../sql-reference/data-types/datetime.md)                                                              | `long (timestamp-millis)` \* |
| `long (timestamp-micros)` \**               | [DateTime64(6)](../sql-reference/data-types/datetime.md)                                                              | `long (timestamp-micros)` \* |

\* `bytes` is default, controlled by [output_format_avro_string_column_pattern](../operations/settings/settings.md#settings-output_format_avro_string_column_pattern)
\** [Avro logical types](https://avro.apache.org/docs/current/spec.html#Logical+Types)

Unsupported Avro data types: `record` (non-root), `map`

Unsupported Avro logical data types: `time-millis`, `time-micros`, `duration`

### Inserting Data {#inserting-data-1}

To insert data from an Avro file into ClickHouse table:

``` bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

The root schema of input Avro file must be of `record` type.

To find the correspondence between table columns and fields of Avro schema ClickHouse compares their names. This comparison is case-sensitive.
Unused fields are skipped.

Data types of ClickHouse table columns can differ from the corresponding fields of the Avro data inserted. When inserting data, ClickHouse interprets data types according to the table above and then [casts](../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) the data to corresponding column type.

### Selecting Data {#selecting-data-1}

To select data from ClickHouse table into an Avro file:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

Column names must:

-   start with `[A-Za-z_]`
-   subsequently contain only `[A-Za-z0-9_]`

Output Avro file compression and sync interval can be configured with [output_format_avro_codec](../operations/settings/settings.md#settings-output_format_avro_codec) and [output_format_avro_sync_interval](../operations/settings/settings.md#settings-output_format_avro_sync_interval) respectively.

## AvroConfluent {#data-format-avro-confluent}

AvroConfluent supports decoding single-object Avro messages commonly used with [Kafka](https://kafka.apache.org/) and [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html).

Each Avro message embeds a schema id that can be resolved to the actual schema with help of the Schema Registry.

Schemas are cached once resolved.

Schema Registry URL is configured with [format_avro_schema_registry_url](../operations/settings/settings.md#format_avro_schema_registry_url).

### Data Types Matching {#data_types-matching-1}

Same as [Avro](#data-format-avro).

### Usage {#usage}

To quickly verify schema resolution you can use [kafkacat](https://github.com/edenhill/kafkacat) with [clickhouse-local](../operations/utilities/clickhouse-local.md):

``` bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```

To use `AvroConfluent` with [Kafka](../engines/table-engines/integrations/kafka.md):

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

SET format_avro_schema_registry_url = 'http://schema-registry';

SELECT * FROM topic1_stream;
```

!!! note "Warning"
    Setting `format_avro_schema_registry_url` needs to be configured in `users.xml` to maintain it’s value after a restart. Also you can use the `format_avro_schema_registry_url` setting of the `Kafka` table engine.

## Parquet {#data-format-parquet}

[Apache Parquet](https://parquet.apache.org/) is a columnar storage format widespread in the Hadoop ecosystem. ClickHouse supports read and write operations for this format.

### Data Types Matching {#data_types-matching-2}

The table below shows supported data types and how they match ClickHouse [data types](../sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| Parquet data type (`INSERT`) | ClickHouse data type                                      | Parquet data type (`SELECT`) |
|------------------------------|-----------------------------------------------------------|------------------------------|
| `UINT8`, `BOOL`              | [UInt8](../sql-reference/data-types/int-uint.md)          | `UINT8`                      |
| `INT8`                       | [Int8](../sql-reference/data-types/int-uint.md)           | `INT8`                       |
| `UINT16`                     | [UInt16](../sql-reference/data-types/int-uint.md)         | `UINT16`                     |
| `INT16`                      | [Int16](../sql-reference/data-types/int-uint.md)          | `INT16`                      |
| `UINT32`                     | [UInt32](../sql-reference/data-types/int-uint.md)         | `UINT32`                     |
| `INT32`                      | [Int32](../sql-reference/data-types/int-uint.md)          | `INT32`                      |
| `UINT64`                     | [UInt64](../sql-reference/data-types/int-uint.md)         | `UINT64`                     |
| `INT64`                      | [Int64](../sql-reference/data-types/int-uint.md)          | `INT64`                      |
| `FLOAT`, `HALF_FLOAT`        | [Float32](../sql-reference/data-types/float.md)           | `FLOAT`                      |
| `DOUBLE`                     | [Float64](../sql-reference/data-types/float.md)           | `DOUBLE`                     |
| `DATE32`                     | [Date](../sql-reference/data-types/date.md)               | `UINT16`                     |
| `DATE64`, `TIMESTAMP`        | [DateTime](../sql-reference/data-types/datetime.md)       | `UINT32`                     |
| `STRING`, `BINARY`           | [String](../sql-reference/data-types/string.md)           | `BINARY`                     |
| —                            | [FixedString](../sql-reference/data-types/fixedstring.md) | `BINARY`                     |
| `DECIMAL`                    | [Decimal](../sql-reference/data-types/decimal.md)         | `DECIMAL`                    |
| `LIST`                       | [Array](../sql-reference/data-types/array.md)             | `LIST`                       |
| `STRUCT`                     | [Tuple](../sql-reference/data-types/tuple.md)             | `STRUCT`                     |
| `MAP`                        | [Map](../sql-reference/data-types/map.md)                 | `MAP`                        |

Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types also can be nested.

ClickHouse supports configurable precision of `Decimal` type. The `INSERT` query treats the Parquet `DECIMAL` type as the ClickHouse `Decimal128` type.

Unsupported Parquet data types: `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

Data types of ClickHouse table columns can differ from the corresponding fields of the Parquet data inserted. When inserting data, ClickHouse interprets data types according to the table above and then [cast](../sql-reference/functions/type-conversion-functions/#type_conversion_function-cast) the data to that data type which is set for the ClickHouse table column.

### Inserting and Selecting Data {#inserting-and-selecting-data}

You can insert Parquet data from a file into ClickHouse table by the following command:

``` bash
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT Parquet"
```

To insert data into [Nested](../sql-reference/data-types/nested-data-structures/nested.md) columns as an array of structs values you must switch on the [input_format_parquet_import_nested](../operations/settings/settings.md#input_format_parquet_import_nested) setting.

You can select data from a ClickHouse table and save them into some file in the Parquet format by the following command:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Parquet" > {some_file.pq}
```

To exchange data with Hadoop, you can use [HDFS table engine](../engines/table-engines/integrations/hdfs.md).

## Arrow {#data-format-arrow}

[Apache Arrow](https://arrow.apache.org/) comes with two built-in columnar storage formats. ClickHouse supports read and write operations for these formats.

`Arrow` is Apache Arrow’s "file mode" format. It is designed for in-memory random access.

### Data Types Matching {#data_types-matching-arrow}

The table below shows supported data types and how they match ClickHouse [data types](../sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| Arrow data type (`INSERT`) | ClickHouse data type                                | Arrow data type (`SELECT`) |
|----------------------------|-----------------------------------------------------|----------------------------|
| `UINT8`, `BOOL`            | [UInt8](../sql-reference/data-types/int-uint.md)    | `UINT8`                    |
| `INT8`                     | [Int8](../sql-reference/data-types/int-uint.md)     | `INT8`                     |
| `UINT16`                   | [UInt16](../sql-reference/data-types/int-uint.md)   | `UINT16`                   |
| `INT16`                    | [Int16](../sql-reference/data-types/int-uint.md)    | `INT16`                    |
| `UINT32`                   | [UInt32](../sql-reference/data-types/int-uint.md)   | `UINT32`                   |
| `INT32`                    | [Int32](../sql-reference/data-types/int-uint.md)    | `INT32`                    |
| `UINT64`                   | [UInt64](../sql-reference/data-types/int-uint.md)   | `UINT64`                   |
| `INT64`                    | [Int64](../sql-reference/data-types/int-uint.md)    | `INT64`                    |
| `FLOAT`, `HALF_FLOAT`      | [Float32](../sql-reference/data-types/float.md)     | `FLOAT32`                  |
| `DOUBLE`                   | [Float64](../sql-reference/data-types/float.md)     | `FLOAT64`                  |
| `DATE32`                   | [Date](../sql-reference/data-types/date.md)         | `UINT16`                   |
| `DATE64`, `TIMESTAMP`      | [DateTime](../sql-reference/data-types/datetime.md) | `UINT32`                   |
| `STRING`, `BINARY`         | [String](../sql-reference/data-types/string.md)     | `BINARY`                   |
| `STRING`, `BINARY`         | [FixedString](../sql-reference/data-types/fixedstring.md)   | `BINARY`                        |
| `DECIMAL`                  | [Decimal](../sql-reference/data-types/decimal.md)   | `DECIMAL`                  |
| `DECIMAL256`               | [Decimal256](../sql-reference/data-types/decimal.md)| `DECIMAL256`               |
| `LIST`                     | [Array](../sql-reference/data-types/array.md)       | `LIST`                     |
| `STRUCT`                   | [Tuple](../sql-reference/data-types/tuple.md)       | `STRUCT`                 |
| `MAP`                      | [Map](../sql-reference/data-types/map.md)           | `MAP`                    |

Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types also can be nested.

The `DICTIONARY` type is supported for `INSERT` queries, and for `SELECT` queries there is an [output_format_arrow_low_cardinality_as_dictionary](../operations/settings/settings.md#output-format-arrow-low-cardinality-as-dictionary) setting that allows to output [LowCardinality](../sql-reference/data-types/lowcardinality.md) type as a `DICTIONARY` type.

ClickHouse supports configurable precision of the `Decimal` type. The `INSERT` query treats the Arrow `DECIMAL` type as the ClickHouse `Decimal128` type.

Unsupported Arrow data types: `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

The data types of ClickHouse table columns do not have to match the corresponding Arrow data fields. When inserting data, ClickHouse interprets data types according to the table above and then [casts](../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) the data to the data type set for the ClickHouse table column.

### Inserting Data {#inserting-data-arrow}

You can insert Arrow data from a file into ClickHouse table by the following command:

``` bash
$ cat filename.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

To insert data into [Nested](../sql-reference/data-types/nested-data-structures/nested.md) columns as an array of structs values you must switch on the [input_format_arrow_import_nested](../operations/settings/settings.md#input_format_arrow_import_nested) setting.

### Selecting Data {#selecting-data-arrow}

You can select data from a ClickHouse table and save them into some file in the Arrow format by the following command:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Arrow" > {filename.arrow}
```

## ArrowStream {#data-format-arrow-stream}

`ArrowStream` is Apache Arrow’s “stream mode” format. It is designed for in-memory stream processing.

## ORC {#data-format-orc}

[Apache ORC](https://orc.apache.org/) is a columnar storage format widespread in the [Hadoop](https://hadoop.apache.org/) ecosystem.

### Data Types Matching {#data_types-matching-3}

The table below shows supported data types and how they match ClickHouse [data types](../sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| ORC data type (`INSERT`) | ClickHouse data type                                | ORC data type (`SELECT`) |
|--------------------------|-----------------------------------------------------|--------------------------|
| `UINT8`, `BOOL`          | [UInt8](../sql-reference/data-types/int-uint.md)    | `UINT8`                  |
| `INT8`                   | [Int8](../sql-reference/data-types/int-uint.md)     | `INT8`                   |
| `UINT16`                 | [UInt16](../sql-reference/data-types/int-uint.md)   | `UINT16`                 |
| `INT16`                  | [Int16](../sql-reference/data-types/int-uint.md)    | `INT16`                  |
| `UINT32`                 | [UInt32](../sql-reference/data-types/int-uint.md)   | `UINT32`                 |
| `INT32`                  | [Int32](../sql-reference/data-types/int-uint.md)    | `INT32`                  |
| `UINT64`                 | [UInt64](../sql-reference/data-types/int-uint.md)   | `UINT64`                 |
| `INT64`                  | [Int64](../sql-reference/data-types/int-uint.md)    | `INT64`                  |
| `FLOAT`, `HALF_FLOAT`    | [Float32](../sql-reference/data-types/float.md)     | `FLOAT`                  |
| `DOUBLE`                 | [Float64](../sql-reference/data-types/float.md)     | `DOUBLE`                 |
| `DATE32`                 | [Date](../sql-reference/data-types/date.md)         | `DATE32`                 |
| `DATE64`, `TIMESTAMP`    | [DateTime](../sql-reference/data-types/datetime.md) | `TIMESTAMP`              |
| `STRING`, `BINARY`       | [String](../sql-reference/data-types/string.md)     | `BINARY`                 |
| `DECIMAL`                | [Decimal](../sql-reference/data-types/decimal.md)   | `DECIMAL`                |
| `LIST`                   | [Array](../sql-reference/data-types/array.md)       | `LIST`                   |
| `STRUCT`                 | [Tuple](../sql-reference/data-types/tuple.md)       | `STRUCT`                 |
| `MAP`                    | [Map](../sql-reference/data-types/map.md)           | `MAP`                    |

Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types also can be nested.

ClickHouse supports configurable precision of the `Decimal` type. The `INSERT` query treats the ORC `DECIMAL` type as the ClickHouse `Decimal128` type.

Unsupported ORC data types: `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

The data types of ClickHouse table columns do not have to match the corresponding ORC data fields. When inserting data, ClickHouse interprets data types according to the table above and then [casts](../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) the data to the data type set for the ClickHouse table column.

### Inserting Data {#inserting-data-2}

You can insert ORC data from a file into ClickHouse table by the following command:

``` bash
$ cat filename.orc | clickhouse-client --query="INSERT INTO some_table FORMAT ORC"
```

To insert data into [Nested](../sql-reference/data-types/nested-data-structures/nested.md) columns as an array of structs values you must switch on the [input_format_orc_import_nested](../operations/settings/settings.md#input_format_orc_import_nested) setting.

### Selecting Data {#selecting-data-2}

You can select data from a ClickHouse table and save them into some file in the ORC format by the following command:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT ORC" > {filename.orc}
```

To exchange data with Hadoop, you can use [HDFS table engine](../engines/table-engines/integrations/hdfs.md).

## LineAsString {#lineasstring}

In this format, every line of input data is interpreted as a single string value. This format can only be parsed for table with a single field of type [String](../sql-reference/data-types/string.md). The remaining columns must be set to [DEFAULT](../sql-reference/statements/create/table.md#default) or [MATERIALIZED](../sql-reference/statements/create/table.md#materialized), or omitted.

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

- `format_regexp` — [String](../sql-reference/data-types/string.md). Contains regular expression in the [re2](https://github.com/google/re2/wiki/Syntax) format.
- `format_regexp_escaping_rule` — [String](../sql-reference/data-types/string.md). The following escaping rules are supported:
    - CSV (similarly to [CSV](#csv))
    - JSON (similarly to [JSONEachRow](#jsoneachrow))
    - Escaped (similarly to [TSV](#tabseparated))
    - Quoted (similarly to [Values](#data-format-values))
    - Raw (extracts subpatterns as a whole, no escaping rules)
- `format_regexp_skip_unmatched` — [UInt8](../sql-reference/data-types/int-uint.md). Defines the need to throw an exeption in case the `format_regexp` expression does not match the imported data. Can be set to `0` or `1`.

**Usage**

The regular expression from `format_regexp` setting is applied to every line of imported data. The number of subpatterns in the regular expression must be equal to the number of columns in imported dataset.

Lines of the imported data must be separated by newline character `'\n'` or DOS-style newline `"\r\n"`.

The content of every matched subpattern is parsed with the method of corresponding data type, according to `format_regexp_escaping_rule` setting.

If the regular expression does not match the line and `format_regexp_skip_unmatched` is set to 1, the line is silently skipped. If `format_regexp_skip_unmatched` is set to 0, exception is thrown.

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
$ cat data.tsv | clickhouse-client  --query "INSERT INTO imp_regex_table FORMAT Regexp SETTINGS format_regexp='id: (.+?) array: (.+?) string: (.+?) date: (.+?)', format_regexp_escaping_rule='Escaped', format_regexp_skip_unmatched=0;"
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

If you input or output data via the [client](../interfaces/cli.md) in the [interactive mode](../interfaces/cli.md#cli_usage), the file name specified in the format schema
can contain an absolute path or a path relative to the current directory on the client.
If you use the client in the [batch mode](../interfaces/cli.md#cli_usage), the path to the schema must be relative due to security reasons.

If you input or output data via the [HTTP interface](../interfaces/http.md) the file name specified in the format schema
should be located in the directory specified in [format_schema_path](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-format_schema_path)
in the server configuration.

## Skipping Errors {#skippingerrors}

Some formats such as `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` and `Protobuf` can skip broken row if parsing error occurred and continue parsing from the beginning of next row. See [input_format_allow_errors_num](../operations/settings/settings.md#settings-input_format_allow_errors_num) and
[input_format_allow_errors_ratio](../operations/settings/settings.md#settings-input_format_allow_errors_ratio) settings.
Limitations:
- In case of parsing error `JSONEachRow` skips all data until the new line (or EOF), so rows must be delimited by `\n` to count errors correctly.
- `Template` and `CustomSeparated` use delimiter after the last column and delimiter between rows to find the beginning of next row, so skipping errors works only if at least one of them is not empty.

## RawBLOB {#rawblob}

In this format, all input data is read to a single value. It is possible to parse only a table with a single field of type [String](../sql-reference/data-types/string.md) or similar.
The result is output in binary format without delimiters and escaping. If more than one value is output, the format is ambiguous, and it will be impossible to read the data back.

Below is a comparison of the formats `RawBLOB` and [TabSeparatedRaw](#tabseparatedraw).
`RawBLOB`:
- data is output in binary format, no escaping;
- there are no delimiters between values;
- no newline at the end of each value.
[TabSeparatedRaw] (#tabseparatedraw):
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

| MessagePack data type (`INSERT`)                                   | ClickHouse data type                                      | MessagePack data type (`SELECT`)   |
|--------------------------------------------------------------------|-----------------------------------------------------------|------------------------------------|
| `uint N`, `positive fixint`                                        | [UIntN](../sql-reference/data-types/int-uint.md)          | `uint N`                           |
| `int N`                                                            | [IntN](../sql-reference/data-types/int-uint.md)           | `int N`                            |
| `bool`                                                             | [UInt8](../sql-reference/data-types/int-uint.md)          | `uint 8`                           |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [String](../sql-reference/data-types/string.md)           | `bin 8`, `bin 16`, `bin 32`        |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [FixedString](../sql-reference/data-types/fixedstring.md) | `bin 8`, `bin 16`, `bin 32`        |
| `float 32`                                                         | [Float32](../sql-reference/data-types/float.md)           | `float 32`                         |
| `float 64`                                                         | [Float64](../sql-reference/data-types/float.md)           | `float 64`                         |
| `uint 16`                                                          | [Date](../sql-reference/data-types/date.md)               | `uint 16`                          |
| `uint 32`                                                          | [DateTime](../sql-reference/data-types/datetime.md)       | `uint 32`                          |
| `uint 64`                                                          | [DateTime64](../sql-reference/data-types/datetime.md)     | `uint 64`                          |
| `fixarray`, `array 16`, `array 32`                                 | [Array](../sql-reference/data-types/array.md)             | `fixarray`, `array 16`, `array 32` |
| `fixmap`, `map 16`, `map 32`                                       | [Map](../sql-reference/data-types/map.md)                 | `fixmap`, `map 16`, `map 32`       |

Example:

Writing to a file ".msgpk":

```sql
$ clickhouse-client --query="CREATE TABLE msgpack (array Array(UInt8)) ENGINE = Memory;"
$ clickhouse-client --query="INSERT INTO msgpack VALUES ([0, 1, 2, 3, 42, 253, 254, 255]), ([255, 254, 253, 42, 3, 2, 1, 0])";
$ clickhouse-client --query="SELECT * FROM msgpack FORMAT MsgPack" > tmp_msgpack.msgpk;
```
