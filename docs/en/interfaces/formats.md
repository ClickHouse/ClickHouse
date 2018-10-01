<a name="formats"></a>

# Formats for input and output data

ClickHouse can accept (`INSERT`) and return (`SELECT`) data in various formats.

The table below lists supported formats and how they can be used in `INSERT` and `SELECT` queries.

| Format | INSERT | SELECT |
| ------- | -------- | -------- |
| [TabSeparated](#tabseparated) | ✔ | ✔ |
| [TabSeparatedRaw](#tabseparatedraw) | ✗ | ✔ |
| [TabSeparatedWithNames](#tabseparatedwithnames) | ✔ | ✔ |
| [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes) | ✔ | ✔ |
| [CSV](#csv) | ✔ | ✔ |
| [CSVWithNames](#csvwithnames) | ✔ | ✔ |
| [Values](#values) | ✔ | ✔ |
| [Vertical](#vertical) | ✗ | ✔ |
| [VerticalRaw](#verticalraw) | ✗ | ✔ |
| [JSON](#json) | ✗ | ✔ |
| [JSONCompact](#jsoncompact) | ✗ | ✔ |
| [JSONEachRow](#jsoneachrow) | ✔ | ✔ |
| [TSKV](#tskv) | ✔ | ✔ |
| [Pretty](#pretty) | ✗ | ✔ |
| [PrettyCompact](#prettycompact) | ✗ | ✔ |
| [PrettyCompactMonoBlock](#prettycompactmonoblock) | ✗ | ✔ |
| [PrettyNoEscapes](#prettynoescapes) | ✗ | ✔ |
| [PrettySpace](#prettyspace) | ✗ | ✔ |
| [RowBinary](#rowbinary) | ✔ | ✔ |
| [Native](#native) | ✔ | ✔ |
| [Null](#null) | ✗ | ✔ |
| [XML](#xml) | ✗ | ✔ |
| [CapnProto](#capnproto) | ✔ | ✔ |

<a name="format_capnproto"></a>

## CapnProto

Cap'n Proto is a binary message format similar to Protocol Buffers and Thrift, but not like JSON or MessagePack.

Cap'n Proto messages are strictly typed and not self-describing, meaning they need an external schema description. The schema is applied on the fly and cached for each query.

```sql
SELECT SearchPhrase, count() AS c FROM test.hits
       GROUP BY SearchPhrase FORMAT CapnProto SETTINGS schema = 'schema:Message'
```

Where `schema.capnp` looks like this:

```
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

Schema files are in the file that is located in the directory specified in [ format_schema_path](../operations/server_settings/settings.md#server_settings-format_schema_path) in the server configuration.

Deserialization is effective and usually doesn't increase the system load.
<a name="csv"></a>

## CSV

Comma Separated Values format ([RFC](https://tools.ietf.org/html/rfc4180)).

When formatting, rows are enclosed in double quotes. A double quote inside a string is output as two double quotes in a row. There are no other rules for escaping characters. Date and date-time are enclosed in double quotes. Numbers are output without quotes. Values are separated by a delimiter character, which is `,` by default. The delimiter character is defined in the setting [format_csv_delimiter](../operations/settings/settings.md#format_csv_delimiter). Rows are separated using the Unix line feed (LF). Arrays are serialized in CSV as follows: first the array is serialized to a string as in TabSeparated format, and then the resulting string is output to CSV in double quotes. Tuples in CSV format are serialized as separate columns (that is, their nesting in the tuple is lost).

```
clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

&ast;By default, the delimiter is `,`. See the [format_csv_delimiter](/operations/settings/settings/#format_csv_delimiter) setting for more information.

When parsing, all values can be parsed either with or without quotes. Both double and single quotes are supported. Rows can also be arranged without quotes. In this case, they are parsed up to the delimiter character or line feed (CR or LF). In violation of the RFC, when parsing rows without quotes, the leading and trailing spaces and tabs are ignored. For the line feed, Unix (LF), Windows (CR LF) and Mac OS Classic (CR LF) types are all supported.

`NULL` is formatted as `\N`.

The CSV format supports the output of totals and extremes the same way as `TabSeparated`.

## CSVWithNames

Also prints the header row, similar to `TabSeparatedWithNames`.
<a name="json"></a>

## JSON

Outputs data in JSON format. Besides data tables, it also outputs column names and types, along with some additional information: the total number of output rows, and the number of rows that could have been output if there weren't a LIMIT. Example:

```sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

```json
{
        "meta":
        [
                {
                        "name": "SearchPhrase",
                        "type": "String"
                },
                {
                        "name": "c",
                        "type": "UInt64"
                }
        ],

        "data":
        [
                {
                        "SearchPhrase": "",
                        "c": "8267016"
                },
                {
                        "SearchPhrase": "bathroom interior design",
                        "c": "2166"
                },
                {
                        "SearchPhrase": "yandex",
                        "c": "1655"
                },
                {
                        "SearchPhrase": "spring 2014 fashion",
                        "c": "1549"
                },
                {
                        "SearchPhrase": "freeform photos",
                        "c": "1480"
                }
        ],

        "totals":
        {
                "SearchPhrase": "",
                "c": "8873898"
        },

        "extremes":
        {
                "min":
                {
                        "SearchPhrase": "",
                        "c": "1480"
                },
                "max":
                {
                        "SearchPhrase": "",
                        "c": "8267016"
                }
        },

        "rows": 5,

        "rows_before_limit_at_least": 141137
}
```

The JSON is compatible with JavaScript. To ensure this, some characters are additionally escaped: the slash `/`  is escaped as `\/`; alternative line breaks `U+2028` and `U+2029`, which break some browsers, are escaped as `\uXXXX`. ASCII control characters are escaped: backspace, form feed, line feed, carriage return, and horizontal tab are replaced with `\b`, `\f`, `\n`, `\r`, `\t`  , as well as the remaining bytes in the 00-1F range using `\uXXXX` sequences. Invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences. For compatibility with JavaScript, Int64 and UInt64 integers are enclosed in double quotes  by default. To remove the quotes, you can set the configuration parameter output_format_json_quote_64bit_integers to 0.

`rows` – The total number of output rows.

`rows_before_limit_at_least` The minimal number of rows there would have been without LIMIT. Output only if the query contains LIMIT.
If the query contains GROUP BY, rows_before_limit_at_least is the exact number of rows there would have been without a LIMIT.

`totals` – Total values (when using WITH TOTALS).

`extremes` – Extreme values (when extremes is set to 1).

This format is only appropriate for outputting a query result, but not for parsing (retrieving data to insert in a table).

ClickHouse supports [NULL](../query_language/syntax.md#null-literal), which is displayed as `null`  in the JSON output.

See also the JSONEachRow format.

<a name="jsoncompact"></a>

## JSONCompact

Differs from JSON only in that data rows are output in arrays, not in objects.

Example:

```json
{
        "meta":
        [
                {
                        "name": "SearchPhrase",
                        "type": "String"
                },
                {
                        "name": "c",
                        "type": "UInt64"
                }
        ],

        "data":
        [
                ["", "8267016"],
                ["bathroom interior design", "2166"],
                ["yandex", "1655"],
                ["fashion trends spring 2014", "1549"],
                ["freeform photo", "1480"]
        ],

        "totals": ["","8873898"],

        "extremes":
        {
                "min": ["","1480"],
                "max": ["","8267016"]
        },

        "rows": 5,

        "rows_before_limit_at_least": 141137
}
```

This format is only appropriate for outputting a query result, but not for parsing (retrieving data to insert in a table).
See also the `JSONEachRow` format.
<a name="jsoneachrow"></a>

## JSONEachRow

Outputs data as separate JSON objects for each row (newline delimited JSON).

```json
{"SearchPhrase":"","count()":"8267016"}
{"SearchPhrase": "bathroom interior design","count()": "2166"}
{"SearchPhrase":"yandex","count()":"1655"}
{"SearchPhrase":"2014 spring fashion","count()":"1549"}
{"SearchPhrase":"freeform photo","count()":"1480"}
{"SearchPhrase":"angelina jolie","count()":"1245"}
{"SearchPhrase":"omsk","count()":"1112"}
{"SearchPhrase":"photos of dog breeds","count()":"1091"}
{"SearchPhrase":"curtain designs","count()":"1064"}
{"SearchPhrase":"baku","count()":"1000"}
```

Unlike the JSON format, there is no substitution of invalid UTF-8 sequences. Any set of bytes can be output in the rows. This is necessary so that data can be formatted without losing any information. Values are escaped in the same way as for JSON.

For parsing, any order is supported for the values of different columns. It is acceptable for some values to be omitted – they are treated as equal to their default values. In this case, zeros and blank rows are used as default values. Complex values that could be specified in the table are not supported as defaults. Whitespace between elements is ignored. If a comma is placed after the objects, it is ignored. Objects don't necessarily have to be separated by new lines.
<a name="native"></a>

## Native

The most efficient format. Data is written and read by blocks in binary format. For each block, the number of rows, number of columns, column names and types, and parts of columns in this block are recorded one after another. In other words, this format is "columnar" – it doesn't convert columns to rows. This is the format used in the native interface for interaction between servers, for using the command-line client, and for C++ clients.

You can use this format to quickly generate dumps that can only be read by the ClickHouse DBMS. It doesn't make sense to work with this format yourself.
<a name="null"></a>

## Null

Nothing is output. However, the query is processed, and when using the command-line client, data is transmitted to the client. This is used for tests, including productivity testing.
Obviously, this format is only appropriate for output, not for parsing.
<a name="pretty"></a>

## Pretty

Outputs data as Unicode-art tables, also using ANSI-escape sequences for setting colors in the terminal.
A full grid of the table is drawn, and each row occupies two lines in the terminal.
Each result block is output as a separate table. This is necessary so that blocks can be output without buffering results (buffering would be necessary in order to pre-calculate the visible width of all the values).

[NULL](../query_language/syntax.md#null-literal) is output as `ᴺᵁᴸᴸ`.

```sql
SELECT * FROM t_null
```

```
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

To avoid dumping too much data to the terminal, only the first 10,000 rows are printed. If the number of rows is greater than or equal to 10,000, the message "Showed first 10 000" is printed.
This format is only appropriate for outputting a query result, but not for parsing (retrieving data to insert in a table).

The Pretty format supports outputting total values (when using WITH TOTALS) and extremes (when 'extremes' is set to 1). In these cases, total values and extreme values are output after the main data, in separate tables. Example (shown for the PrettyCompact format):

```sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT PrettyCompact
```

```text
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
│ 0000-00-00 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

<a name="prettycompact"></a>

## PrettyCompact

Differs from `Pretty` in that the grid is drawn between rows and the result is more compact.
This format is used by default in the command-line client in interactive mode.
<a name="prettycompactmonoblock"></a>

## PrettyCompactMonoBlock

Differs from [PrettyCompact](#prettycompact) in that up to 10,000 rows are buffered, then output as a single table, not by blocks.
<a name="prettynoescapes"></a>

## PrettyNoEscapes

Differs from Pretty in that ANSI-escape sequences aren't used. This is necessary for displaying this format in a browser, as well as for using the 'watch' command-line utility.

Example:

```bash
watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

You can use the HTTP interface for displaying in the browser.

### PrettyCompactNoEscapes

The same as the previous setting.

### PrettySpaceNoEscapes

The same as the previous setting.
<a name="prettyspace"></a>

## PrettySpace

Differs from [PrettyCompact](#prettycompact) in that whitespace (space characters) is used instead of the grid.
<a name="rowbinary"></a>

## RowBinary

Formats and parses data by row in binary format. Rows and values are listed consecutively, without separators.
This format is less efficient than the Native format, since it is row-based.

Integers use fixed-length little endian representation. For example, UInt64 uses 8 bytes.
DateTime is represented as UInt32 containing the Unix timestamp as the value.
Date is represented as a UInt16 object that contains the number of days since 1970-01-01 as the value.
String is represented as a varint length (unsigned [LEB128](https://en.wikipedia.org/wiki/LEB128)), followed by the bytes of the string.
FixedString is represented simply as a sequence of bytes.

Array is represented as a varint length (unsigned [LEB128](https://en.wikipedia.org/wiki/LEB128)), followed by successive elements of the array.

For [NULL](../query_language/syntax.md#null-literal) support, an additional byte containing 1 or 0 is added before each [Nullable](../data_types/nullable.md#data_type-nullable) value. If 1, then the value is `NULL` and this byte is interpreted as a separate value. If 0, the value after the byte is not `NULL`.

<a name="tabseparated"></a>

## TabSeparated

In TabSeparated format, data is written by row. Each row contains values separated by tabs. Each value is follow by a tab, except the last value in the row, which is followed by a line feed. Strictly Unix line feeds are assumed everywhere. The last row also must contain a line feed at the end. Values are written in text format, without enclosing quotation marks, and with special characters escaped.

This format is also available under the name `TSV`.

The `TabSeparated` format is convenient for processing data using custom programs and scripts. It is used by default in the HTTP interface, and in the command-line client's batch mode. This format also allows transferring data between different DBMSs. For example, you can get a dump from MySQL and upload it to ClickHouse, or vice versa.

The `TabSeparated` format supports outputting total values (when using WITH TOTALS) and extreme values (when 'extremes' is set to 1). In these cases, the total values and extremes are output after the main data. The main result, total values, and extremes are separated from each other by an empty line. Example:

```sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated``
```

```text
2014-03-17      1406958
2014-03-18      1383658
2014-03-19      1405797
2014-03-20      1353623
2014-03-21      1245779
2014-03-22      1031592
2014-03-23      1046491

0000-00-00      8873898

2014-03-17      1031592
2014-03-23      1406958
```

## Data formatting

Integer numbers are written in decimal form. Numbers can contain an extra "+" character at the beginning (ignored when parsing, and not recorded when formatting). Non-negative numbers can't contain the negative sign. When reading, it is allowed to parse an empty string as a zero, or (for signed types) a string consisting of just a minus sign as a zero. Numbers that do not fit into the corresponding data type may be parsed as a different number, without an error message.

Floating-point numbers are written in decimal form. The dot is used as the decimal separator. Exponential entries are supported, as are 'inf', '+inf', '-inf', and 'nan'. An entry of floating-point numbers may begin or end with a decimal point.
During formatting, accuracy may be lost on floating-point numbers.
During parsing, it is not strictly required to read the nearest machine-representable number.

Dates are written in YYYY-MM-DD format and parsed in the same format, but with any characters as separators.
Dates with times are written in the format YYYY-MM-DD hh:mm:ss and parsed in the same format, but with any characters as separators.
This all occurs in the system time zone at the time the client or server starts (depending on which one formats data). For dates with times, daylight saving time is not specified. So if a dump has times during daylight saving time, the dump does not unequivocally match the data, and parsing will select one of the two times.
During a read operation, incorrect dates and dates with times can be parsed with natural overflow or as null dates and times, without an error message.

As an exception, parsing dates with times is also supported in Unix timestamp format, if it consists of exactly 10 decimal digits. The result is not time zone-dependent. The formats YYYY-MM-DD hh:mm:ss and NNNNNNNNNN are differentiated automatically.

Strings are output with backslash-escaped special characters. The following escape sequences are used for output: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. Parsing also supports the sequences `\a`, `\v`, and `\xHH`  (hex escape sequences) and any `\c` sequences, where `c` is any character (these sequences are converted to `c`). Thus, reading data supports formats where a line feed can be written as `\n`  or `\`, or as a line feed. For example, the string `Hello world` with a line feed between the words instead of a space can be parsed in any of the following variations:

```text
Hello\nworld

Hello\
world
```

The second variant is supported because MySQL uses it when writing tab-separated dumps.

The minimum set of characters that you need to escape when passing data in TabSeparated format: tab, line feed (LF) and backslash.

Only a small set of symbols are escaped. You can easily stumble onto a string value that your terminal will ruin in output.

Arrays are written as a list of comma-separated values in square brackets. Number items in the array are fomratted as normally, but dates, dates with times, and strings are written in single quotes with the same escaping rules as above.

[NULL](../query_language/syntax.md#null-literal) is formatted as `\N`.

<a name="tabseparatedraw"></a>

## TabSeparatedRaw

Differs from `TabSeparated` format in that the rows are written without escaping.
This format is only appropriate for outputting a query result, but not for parsing (retrieving data to insert in a table).

This format is also available under the name `TSVRaw`.
<a name="tabseparatedwithnames"></a>

## TabSeparatedWithNames

Differs from the `TabSeparated` format in that the column names are written in the first row.
During parsing, the first row is completely ignored. You can't use column names to determine their position or to check their correctness.
(Support for parsing the header row may be added in the future.)

This format is also available under the name `TSVWithNames`.
<a name="tabseparatedwithnamesandtypes"></a>

## TabSeparatedWithNamesAndTypes

Differs from the `TabSeparated` format in that the column names are written to the first row, while the column types are in the second row.
During parsing, the first and second rows are completely ignored.

This format is also available under the name `TSVWithNamesAndTypes`.
<a name="tskv"></a>

## TSKV

Similar to TabSeparated, but outputs a value in name=value format. Names are escaped the same way as in TabSeparated format, and the = symbol is also escaped.

```text
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

[NULL](../query_language/syntax.md#null-literal) is formatted as `\N`.

```sql
SELECT * FROM t_null FORMAT TSKV
```

```
x=1	y=\N
```

When there is a large number of small columns, this format is ineffective, and there is generally no reason to use it. It is used in some departments of Yandex.

Both data output and parsing are supported in this format. For parsing, any order is supported for the values of different columns. It is acceptable for some values to be omitted – they are treated as equal to their default values. In this case, zeros and blank rows are used as default values. Complex values that could be specified in the table are not supported as defaults.

Parsing allows the presence of the additional field `tskv` without the equal sign or a value. This field is ignored.

## Values

Prints every row in brackets. Rows are separated by commas. There is no comma after the last row. The values inside the brackets are also comma-separated. Numbers are output in decimal format without quotes. Arrays are output in square brackets. Strings, dates, and dates with times are output in quotes. Escaping rules and parsing are similar to the [TabSeparated](#tabseparated) format. During formatting, extra spaces aren't inserted, but during parsing, they are allowed and skipped (except for spaces inside array values, which are not allowed). [NULL](../query_language/syntax.md#null-literal) is represented as `NULL`.

The minimum set of characters that you need to escape when passing data in Values ​​format: single quotes and backslashes.

This is the format that is used in `INSERT INTO t VALUES ...`, but you can also use it for formatting query results.

<a name="vertical"></a>

## Vertical

Prints each value on a separate line with the column name specified. This format is convenient for printing just one or a few rows, if each row consists of a large number of columns.

[NULL](../query_language/syntax.md#null-literal) is output as `ᴺᵁᴸᴸ`.

Example:

```sql
SELECT * FROM t_null FORMAT Vertical
```

```
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

This format is only appropriate for outputting a query result, but not for parsing (retrieving data to insert in a table).

<a name="verticalraw"></a>

## VerticalRaw

Differs from `Vertical` format in that the rows are not escaped.
This format is only appropriate for outputting a query result, but not for parsing (retrieving data to insert in a table).

Examples:

```
:) SHOW CREATE TABLE geonames FORMAT VerticalRaw;
Row 1:
──────
statement: CREATE TABLE default.geonames ( geonameid UInt32, date Date DEFAULT CAST('2017-12-08' AS Date)) ENGINE = MergeTree(date, geonameid, 8192)

:) SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT VerticalRaw;
Row 1:
──────
test: string with 'quotes' and   with some special
 characters
```

Compare with the Vertical format:

```
:) SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical;
Row 1:
──────
test: string with \'quotes\' and \t with some special \n characters
```

<a name="xml"></a>

## XML

XML format is suitable only for output, not for parsing. Example:

```xml
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

If the column name does not have an acceptable format, just 'field' is used as the element name. In general, the XML structure follows the JSON structure.
Just as for JSON, invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences.

In string values, the characters `<` and `&` are escaped as `<` and `&`.

Arrays are output as `<array><elem>Hello</elem><elem>World</elem>...</array>`,and tuples as `<tuple><elem>Hello</elem><elem>World</elem>...</tuple>`.
