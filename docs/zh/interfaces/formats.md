# 输入输出格式 {#formats}

ClickHouse 可以接受多种数据格式，可以在 (`INSERT`) 以及 (`SELECT`) 请求中使用。

下列表格列出了支持的数据格式以及在 (`INSERT`) 以及 (`SELECT`) 请求中使用它们的方式。

| 格式                                                            | INSERT | SELECT |
|-----------------------------------------------------------------|--------|--------|
| [TabSeparated](#tabseparated)                                   | ✔      | ✔      |
| [TabSeparatedRaw](#tabseparatedraw)                             | ✗      | ✔      |
| [TabSeparatedWithNames](#tabseparatedwithnames)                 | ✔      | ✔      |
| [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes) | ✔      | ✔      |
| [Template](#format-template)                                    | ✔      | ✔      |
| [TemplateIgnoreSpaces](#templateignorespaces)                   | ✔      | ✗      |
| [CSV](#csv)                                                     | ✔      | ✔      |
| [CSVWithNames](#csvwithnames)                                   | ✔      | ✔      |
| [CustomSeparated](#format-customseparated)                      | ✔      | ✔      |
| [Values](#data-format-values)                                   | ✔      | ✔      |
| [Vertical](#vertical)                                           | ✗      | ✔      |
| VerticalRaw                                                     | ✗      | ✔      |
| [JSON](#json)                                                   | ✗      | ✔      |
| [JSONCompact](#jsoncompact)                                     | ✗      | ✔      |
| [JSONEachRow](#jsoneachrow)                                     | ✔      | ✔      |
| [TSKV](#tskv)                                                   | ✔      | ✔      |
| [Pretty](#pretty)                                               | ✗      | ✔      |
| [PrettyCompact](#prettycompact)                                 | ✗      | ✔      |
| [PrettyCompactMonoBlock](#prettycompactmonoblock)               | ✗      | ✔      |
| [PrettyNoEscapes](#prettynoescapes)                             | ✗      | ✔      |
| [PrettySpace](#prettyspace)                                     | ✗      | ✔      |
| [Protobuf](#protobuf)                                           | ✔      | ✔      |
| [Avro](#data-format-avro)                                       | ✔      | ✔      |
| [AvroConfluent](#data-format-avro-confluent)                    | ✔      | ✗      |
| [Parquet](#data-format-parquet)                                 | ✔      | ✔      |
| [ORC](#data-format-orc)                                         | ✔      | ✗      |
| [RowBinary](#rowbinary)                                         | ✔      | ✔      |
| [RowBinaryWithNamesAndTypes](#rowbinarywithnamesandtypes)       | ✔      | ✔      |
| [Native](#native)                                               | ✔      | ✔      |
| [Null](#null)                                                   | ✗      | ✔      |
| [XML](#xml)                                                     | ✗      | ✔      |
| [CapnProto](#capnproto)                                         | ✔      | ✔      |

## TabSeparated {#tabseparated}

在 TabSeparated 格式中，数据按行写入。每行包含由制表符分隔的值。除了行中的最后一个值（后面紧跟换行符）之外，每个值都跟随一个制表符。 在任何地方都可以使用严格的 Unix 命令行。最后一行还必须在最后包含换行符。值以文本格式编写，不包含引号，并且要转义特殊字符。

这种格式也可以用 `TSV` 来表示。

TabSeparated 格式非常方便用于自定义程序或脚本处理数据。HTTP 客户端接口默认会用这种格式，命令行客户端批量模式下也会用这种格式。这种格式允许在不同数据库之间传输数据。例如，从 MYSQL 中导出数据然后导入到 ClickHouse 中，反之亦然。

TabSeparated 格式支持输出数据总值（当使用 WITH TOTALS） 以及极值（当 ‘extremes’ 设置是1）。这种情况下，总值和极值输出在主数据的后面。主要的数据，总值，极值会以一个空行隔开，例如：

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated``
```

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

### 数据解析方式 {#shu-ju-jie-xi-fang-shi}

整数以十进制形式写入。数字在开头可以包含额外的 `+` 字符（解析时忽略，格式化时不记录）。非负数不能包含负号。 读取时，允许将空字符串解析为零，或者（对于带符号的类型）将仅包含负号的字符串解析为零。 不符合相应数据类型的数字可能会被解析为不同的数字，而不会显示错误消息。

浮点数以十进制形式写入。点号用作小数点分隔符。支持指数等符号，如’inf’，‘+ inf’，‘-inf’和’nan’。 浮点数的输入可以以小数点开始或结束。
格式化的时候，浮点数的精确度可能会丢失。
解析的时候，没有严格需要去读取与机器可以表示的最接近的数值。

日期会以 YYYY-MM-DD 格式写入和解析，但会以任何字符作为分隔符。
带时间的日期会以 YYYY-MM-DD hh:mm:ss 格式写入和解析，但会以任何字符作为分隔符。
这一切都发生在客户端或服务器启动时的系统时区（取决于哪一种格式的数据）。对于具有时间的日期，夏时制时间未指定。 因此，如果转储在夏令时中有时间，则转储不会明确地匹配数据，解析将选择两者之一。
在读取操作期间，不正确的日期和具有时间的日期可以使用自然溢出或空日期和时间进行分析，而不会出现错误消息。

有个例外情况，Unix 时间戳格式（10个十进制数字）也支持使用时间解析日期。结果不是时区相关的。格式 YYYY-MM-DD hh:mm:ss和 NNNNNNNNNN 会自动区分。

字符串以反斜线转义的特殊字符输出。 以下转义序列用于输出：`\b`，`\f`，`\r`，`\n`，`\t`，`\0`，`\'`，`\\`。 解析还支持`\a`，`\v`和`\xHH`（十六进制转义字符）和任何`\c`字符，其中`c`是任何字符（这些序列被转换为`c`）。 因此，读取数据支持可以将换行符写为`\n`或`\`的格式，或者换行。例如，字符串 `Hello world` 在单词之间换行而不是空格可以解析为以下任何形式：

    Hello\nworld

    Hello\
    world

第二种形式是支持的，因为 MySQL 读取 tab-separated 格式数据集的时候也会使用它。

在 TabSeparated 格式中传递数据时需要转义的最小字符集为：Tab，换行符（LF）和反斜杠。

只有一小组符号会被转义。你可以轻易地找到一个字符串值，但这不会正常在你的终端显示。

数组写在方括号内的逗号分隔值列表中。 通常情况下，数组中的数字项目会被拼凑，但日期，带时间的日期以及字符串将使用与上面相同的转义规则用单引号引起来。

[NULL](../query_language/syntax.md) 将输出为 `\N`。

## TabSeparatedRaw {#tabseparatedraw}

与 `TabSeparated` 格式不一样的是，行数据是不会被转义的。
该格式仅适用于输出查询结果，但不适用于解析输入（将数据插入到表中）。

这种格式也可以使用名称 `TSVRaw` 来表示。

## TabSeparatedWithNames {#tabseparatedwithnames}

与 `TabSeparated` 格式不一样的是，第一行会显示列的名称。
在解析过程中，第一行完全被忽略。您不能使用列名来确定其位置或检查其正确性。
（未来可能会加入解析头行的功能）

这种格式也可以使用名称 `TSVWithNames` 来表示。

## TabSeparatedWithNamesAndTypes {#tabseparatedwithnamesandtypes}

与 `TabSeparated` 格式不一样的是，第一行会显示列的名称，第二行会显示列的类型。
在解析过程中，第一行和第二行完全被忽略。

这种格式也可以使用名称 `TSVWithNamesAndTypes` 来表示。

## Template {#format-template}

This format allows to specify a custom format string with placeholders for values with specified escaping rule.

It uses settings `format_schema`, `format_schema_rows`, `format_schema_rows_between_delimiter` and some settings of other formats (e.g. `output_format_json_quote_64bit_integers` when using `JSON` escaping, see further)

Format string `format_schema_rows` specifies rows format with the following syntax:

`delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N`,

    where `delimiter_i` is a delimiter between values (`$` symbol can be escaped as `$$`),
    `column_i` is a name of a column whose values are to be selected or inserted (if empty, then column will be skipped),
    `serializeAs_i` is an escaping rule for the column values. The following escaping rules are supported:

    - `CSV`, `JSON`, `XML` (similarly to the formats of the same names)
    - `Escaped` (similarly to `TSV`)
    - `Quoted` (similarly to `Values`)
    - `Raw` (without escaping, similarly to `TSVRaw`)
    - `None` (no escaping rule, see further)

    If escaping rule is omitted, then`None` will be used. `XML` and `Raw` are suitable only for output.

    So, for the following format string:

      `Search phrase: ${SearchPhrase:Quoted}, count: ${c:Escaped}, ad price: $$${price:JSON};`

    the values of `SearchPhrase`, `c` and `price` columns, which are escaped as `Quoted`, `Escaped` and `JSON` will be printed (for select) or will be expected (for insert) between `Search phrase: `, `, count: `, `, ad price: $` and `;` delimiters respectively. For example:

    `Search phrase: 'bathroom interior design', count: 2166, ad price: $3;`

The `format_schema_rows_between_delimiter` setting specifies delimiter between rows, which is printed (or expected) after every row except the last one (`\n` by default)

Format string `format_schema` has the same syntax as `format_schema_rows` and allows to specify a prefix, a suffix and a way to print some additional information. It contains the following placeholders instead of column names:

-   `data` is the rows with data in `format_schema_rows` format, separated by `format_schema_rows_between_delimiter`. This placeholder must be the first placeholder in the format string.
-   `totals` is the row with total values in `format_schema_rows` format (when using WITH TOTALS)
-   `min` is the row with minimum values in `format_schema_rows` format (when extremes is set to 1)
-   `max` is the row with maximum values in `format_schema_rows` format (when extremes is set to 1)
-   `rows` is the total number of output rows
-   `rows_before_limit` is the minimal number of rows there would have been without LIMIT. Output only if the query contains LIMIT. If the query contains GROUP BY, rows\_before\_limit\_at\_least is the exact number of rows there would have been without a LIMIT.
-   `time` is the request execution time in seconds
-   `rows_read` is the number of rows have been read
-   `bytes_read` is the number of bytes (uncompressed) have been read

The placeholders `data`, `totals`, `min` and `max` must not have escaping rule specified (or `None` must be specified explicitly). The remaining placeholders may have any escaping rule specified.
If the `format_schema` setting is an empty string, `${data}` is used as default value.
For insert queries format allows to skip some columns or some fields if prefix or suffix (see example).

`Select` example:

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5
FORMAT Template
SETTINGS format_schema = '<!DOCTYPE HTML>
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
</html>',
format_schema_rows = '<tr> <td>${SearchPhrase:XML}</td> <td>${с:XML}</td> </tr>',
format_schema_rows_between_delimiter = '\n    '
```

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

`Insert` example:

    Some header
    Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
    Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
    Total rows: 2

``` sql
INSERT INTO UserActivity FORMAT Template SETTINGS
format_schema = 'Some header\n${data}\nTotal rows: ${:CSV}\n',
format_schema_rows = 'Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}'
```

`PageViews`, `UserID`, `Duration` and `Sign` inside placeholders are names of columns in the table. Values after `Useless field` in rows and after `\nTotal rows:` in suffix will be ignored.
All delimiters in the input data must be strictly equal to delimiters in specified format strings.

## TemplateIgnoreSpaces {#templateignorespaces}

This format is suitable only for input.
Similar to `Template`, but skips whitespace characters between delimiters and values in the input stream. However, if format strings contain whitespace characters, these characters will be expected in the input stream. Also allows to specify empty placeholders (`${}` or `${:None}`) to split some delimiter into separate parts to ignore spaces between them. Such placeholders are used only for skipping whitespace characters.
It’s possible to read `JSON` using this format, if values of columns have the same order in all rows. For example, the following request can be used for inserting data from output example of format [JSON](#json):

``` sql
INSERT INTO table_name FORMAT TemplateIgnoreSpaces SETTINGS
format_schema = '{${}"meta"${}:${:JSON},${}"data"${}:${}[${data}]${},${}"totals"${}:${:JSON},${}"extremes"${}:${:JSON},${}"rows"${}:${:JSON},${}"rows_before_limit_at_least"${}:${:JSON}${}}',
format_schema_rows = '{${}"SearchPhrase"${}:${}${phrase:JSON}${},${}"c"${}:${}${cnt:JSON}${}}',
format_schema_rows_between_delimiter = ','
```

## TSKV {#tskv}

与 `TabSeparated` 格式类似，但它输出的是 `name=value` 的格式。名称会和 `TabSeparated` 格式一样被转义，`=` 字符也会被转义。

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

[NULL](../query_language/syntax.md) 输出为 `\N`。

``` sql
SELECT * FROM t_null FORMAT TSKV
```

    x=1 y=\N

当有大量的小列时，这种格式是低效的，通常没有理由使用它。它被用于 Yandex 公司的一些部门。

数据的输出和解析都支持这种格式。对于解析，任何顺序都支持不同列的值。可以省略某些值，用 `-` 表示， 它们被视为等于它们的默认值。在这种情况下，零和空行被用作默认值。作为默认值，不支持表中指定的复杂值。

对于不带等号或值，可以用附加字段 `tskv` 来表示，这种在解析上是被允许的。这样的话该字段被忽略。

## CSV {#csv}

按逗号分隔的数据格式([RFC](https://tools.ietf.org/html/rfc4180))。

格式化的时候，行是用双引号括起来的。字符串中的双引号会以两个双引号输出，除此之外没有其他规则来做字符转义了。日期和时间也会以双引号包括。数字的输出不带引号。值由一个单独的字符隔开，这个字符默认是 `,`。行使用 Unix 换行符（LF）分隔。 数组序列化成 CSV 规则如下：首先将数组序列化为 TabSeparated 格式的字符串，然后将结果字符串用双引号包括输出到 CSV。CSV 格式的元组被序列化为单独的列（即它们在元组中的嵌套关系会丢失）。

    clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv

\*默认情况下间隔符是 `,` ，在 [format\_csv\_delimiter](../operations/settings/settings.md#settings-format_csv_delimiter) 中可以了解更多间隔符配置。

解析的时候，可以使用或不使用引号来解析所有值。支持双引号和单引号。行也可以不用引号排列。 在这种情况下，它们被解析为逗号或换行符（CR 或 LF）。在解析不带引号的行时，若违反 RFC 规则，会忽略前导和尾随的空格和制表符。 对于换行，全部支持 Unix（LF），Windows（CR LF）和 Mac OS Classic（CR LF）。

`NULL` 将输出为 `\N`。

CSV 格式是和 TabSeparated 一样的方式输出总数和极值。

## CSVWithNames {#csvwithnames}

会输出带头部行，和 `TabSeparatedWithNames` 一样。

## CustomSeparated {#format-customseparated}

Similar to [Template](#format-template), but it prints or reads all columns and uses escaping rule from setting `format_custom_escaping_rule` and delimiters from settings `format_custom_field_delimiter`, `format_custom_row_before_delimiter`, `format_custom_row_after_delimiter`, `format_custom_row_between_delimiter`, `format_custom_result_before_delimiter` and `format_custom_result_after_delimiter`, not from format strings.
There is also `CustomSeparatedIgnoreSpaces` format, which is similar to `TemplateIgnoreSpaces`.

## JSON {#json}

以 JSON 格式输出数据。除了数据表之外，它还输出列名称和类型以及一些附加信息：输出行的总数以及在没有 LIMIT 时可以输出的行数。 例：

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

``` json
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

JSON 与 JavaScript 兼容。为了确保这一点，一些字符被另外转义：斜线`/`被转义为`\/`; 替代的换行符 `U+2028` 和 `U+2029` 会打断一些浏览器解析，它们会被转义为 `\uXXXX`。 ASCII 控制字符被转义：退格，换页，换行，回车和水平制表符被替换为`\b`，`\f`，`\n`，`\r`，`\t` 作为使用`\uXXXX`序列的00-1F范围内的剩余字节。 无效的 UTF-8 序列更改为替换字符 ，因此输出文本将包含有效的 UTF-8 序列。 为了与 JavaScript 兼容，默认情况下，Int64 和 UInt64 整数用双引号引起来。要除去引号，可以将配置参数 output\_format\_json\_quote\_64bit\_integers 设置为0。

`rows` – 结果输出的行数。

`rows_before_limit_at_least` 去掉 LIMIT 过滤后的最小行总数。 只会在查询包含 LIMIT 条件时输出。
若查询包含 GROUP BY，rows\_before\_limit\_at\_least 就是去掉 LIMIT 后过滤后的准确行数。

`totals` – 总值 （当使用 TOTALS 条件时）。

`extremes` – 极值 （当 extremes 设置为 1时）。

该格式仅适用于输出查询结果，但不适用于解析输入（将数据插入到表中）。

ClickHouse 支持 [NULL](../query_language/syntax.md), 在 JSON 格式中以 `null` 输出来表示.

参考 JSONEachRow 格式。

## JSONCompact {#jsoncompact}

与 JSON 格式不同的是它以数组的方式输出结果，而不是以结构体。

示例：

``` json
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

这种格式仅仅适用于输出结果集，而不适用于解析（将数据插入到表中）。
参考 `JSONEachRow` 格式。

## JSONEachRow {#jsoneachrow}

将数据结果每一行以 JSON 结构体输出（换行分割 JSON 结构体）。

``` json
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

与 JSON 格式不同的是，没有替换无效的UTF-8序列。任何一组字节都可以在行中输出。这是必要的，因为这样数据可以被格式化而不会丢失任何信息。值的转义方式与JSON相同。

对于解析，任何顺序都支持不同列的值。可以省略某些值 - 它们被视为等于它们的默认值。在这种情况下，零和空行被用作默认值。 作为默认值，不支持表中指定的复杂值。元素之间的空白字符被忽略。如果在对象之后放置逗号，它将被忽略。对象不一定必须用新行分隔。

### Usage of Nested Structures {#jsoneachrow-nested}

If you have a table with the [Nested](../data_types/nested_data_structures/nested.md) data type columns, you can insert JSON data having the same structure. Enable this functionality with the [input\_format\_import\_nested\_json](../operations/settings/settings.md#settings-input_format_import_nested_json) setting.

For example, consider the following table:

``` sql
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

As you can find in the `Nested` data type description, ClickHouse treats each component of the nested structure as a separate column, `n.s` and `n.i` for our table. So you can insert the data the following way:

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

To insert data as hierarchical JSON object set [input\_format\_import\_nested\_json=1](../operations/settings/settings.md#settings-input_format_import_nested_json).

``` json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

Without this setting ClickHouse throws the exception.

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

最高性能的格式。 据通过二进制格式的块进行写入和读取。对于每个块，该块中的行数，列数，列名称和类型以及列的部分将被相继记录。 换句话说，这种格式是 «列式»的 - 它不会将列转换为行。 这是用于在服务器之间进行交互的本地界面中使用的格式，用于使用命令行客户端和 C++ 客户端。

您可以使用此格式快速生成只能由 ClickHouse DBMS 读取的格式。但自己处理这种格式是没有意义的。

## Null {#null}

没有输出。但是，查询已处理完毕，并且在使用命令行客户端时，数据将传输到客户端。这仅用于测试，包括生产力测试。
显然，这种格式只适用于输出，不适用于解析。

## Pretty {#pretty}

将数据以表格形式输出，也可以使用 ANSI 转义字符在终端中设置颜色。
它会绘制一个完整的表格，每行数据在终端中占用两行。
每一个结果块都会以单独的表格输出。这是很有必要的，以便结果块不用缓冲结果输出（缓冲在可以预见结果集宽度的时候是很有必要的）。

[NULL](../query_language/syntax.md) 输出为 `ᴺᵁᴸᴸ`。

``` sql
SELECT * FROM t_null
```

    ┌─x─┬────y─┐
    │ 1 │ ᴺᵁᴸᴸ │
    └───┴──────┘

为避免将太多数据传输到终端，只打印前10,000行。 如果行数大于或等于10,000，则会显示消息«Showed first 10 000»。
该格式仅适用于输出查询结果，但不适用于解析输入（将数据插入到表中）。

Pretty格式支持输出总值（当使用 WITH TOTALS 时）和极值（当 `extremes` 设置为1时）。 在这些情况下，总数值和极值在主数据之后以单独的表格形式输出。 示例（以 PrettyCompact 格式显示）：

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT PrettyCompact
```

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

## PrettyCompact {#prettycompact}

与 `Pretty` 格式不一样的是，`PrettyCompact` 去掉了行之间的表格分割线，这样使得结果更加紧凑。这种格式会在交互命令行客户端下默认使用。

## PrettyCompactMonoBlock {#prettycompactmonoblock}

与 `PrettyCompact` 格式不一样的是，它支持 10,000 行数据缓冲，然后输出在一个表格中，不会按照块来区分

## PrettyNoEscapes {#prettynoescapes}

与 `Pretty` 格式不一样的是，它不使用 ANSI 字符转义， 这在浏览器显示数据以及在使用 `watch` 命令行工具是有必要的。

示例：

``` bash
watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

您可以使用 HTTP 接口来获取数据，显示在浏览器中。

### PrettyCompactNoEscapes {#prettycompactnoescapes}

用法类似上述。

### PrettySpaceNoEscapes {#prettyspacenoescapes}

用法类似上述。

## PrettySpace {#prettyspace}

与 `PrettyCompact`(\#prettycompact) 格式不一样的是，它使用空格来代替网格来显示数据。

## RowBinary {#rowbinary}

以二进制格式逐行格式化和解析数据。行和值连续列出，没有分隔符。
这种格式比 Native 格式效率低，因为它是基于行的。

整数使用固定长度的小端表示法。 例如，UInt64 使用8个字节。
DateTime 被表示为 UInt32 类型的Unix 时间戳值。
Date 被表示为 UInt16 对象，它的值为 1970-01-01以来的天数。
字符串表示为 varint 长度（无符号 [LEB128](https://en.wikipedia.org/wiki/LEB128)），后跟字符串的字节数。
FixedString 被简单地表示为一个字节序列。

数组表示为 varint 长度（无符号 [LEB128](https://en.wikipedia.org/wiki/LEB128)），后跟有序的数组元素。

对于 [NULL](../query_language/syntax.md#null-literal) 的支持， 一个为 1 或 0 的字节会加在每个 [Nullable](../data_types/nullable.md) 值前面。如果为 1, 那么该值就是 `NULL`。 如果为 0，则不为 `NULL`。

## RowBinaryWithNamesAndTypes {#rowbinarywithnamesandtypes}

Similar to [RowBinary](#rowbinary), but with added header:

-   [LEB128](https://en.wikipedia.org/wiki/LEB128)-encoded number of columns (N)
-   N `String`s specifying column names
-   N `String`s specifying column types

## Values {#data-format-values}

在括号中打印每一行。行由逗号分隔。最后一行之后没有逗号。括号内的值也用逗号分隔。数字以十进制格式输出，不含引号。 数组以方括号输出。带有时间的字符串，日期和时间用引号包围输出。转义字符的解析规则与 [TabSeparated](#tabseparated) 格式类似。 在格式化过程中，不插入额外的空格，但在解析过程中，空格是被允许并跳过的（除了数组值之外的空格，这是不允许的）。[NULL](../query_language/syntax.md) 为 `NULL`。

以 Values 格式传递数据时需要转义的最小字符集是：单引号和反斜线。

这是 `INSERT INTO t VALUES ...` 中可以使用的格式，但您也可以将其用于查询结果。

## Vertical {#vertical}

使用指定的列名在单独的行上打印每个值。如果每行都包含大量列，则此格式便于打印一行或几行。

[NULL](../query_language/syntax.md) 输出为 `ᴺᵁᴸᴸ`。

示例:

``` sql
SELECT * FROM t_null FORMAT Vertical
```

    Row 1:
    ──────
    x: 1
    y: ᴺᵁᴸᴸ

该格式仅适用于输出查询结果，但不适用于解析输入（将数据插入到表中）。

## VerticalRaw {#verticalraw}

和 `Vertical` 格式不同点在于，行是不会被转义的。
这种格式仅仅适用于输出，但不适用于解析输入（将数据插入到表中）。

示例:

    :) SHOW CREATE TABLE geonames FORMAT VerticalRaw;
    Row 1:
    ──────
    statement: CREATE TABLE default.geonames ( geonameid UInt32, date Date DEFAULT CAST('2017-12-08' AS Date)) ENGINE = MergeTree(date, geonameid, 8192)

    :) SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT VerticalRaw;
    Row 1:
    ──────
    test: string with 'quotes' and   with some special
     characters

和 Vertical 格式相比：

    :) SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical;
    Row 1:
    ──────
    test: string with \'quotes\' and \t with some special \n characters

## XML {#xml}

该格式仅适用于输出查询结果，但不适用于解析输入，示例：

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

如果列名称没有可接受的格式，则仅使用 `field` 作为元素名称。 通常，XML 结构遵循 JSON 结构。
就像JSON一样，将无效的 UTF-8 字符都作替换，以便输出文本将包含有效的 UTF-8 字符序列。

在字符串值中，字符 `<` 和 `＆` 被转义为 `<` 和 `＆`。

数组输出为 `<array> <elem> Hello </ elem> <elem> World </ elem> ... </ array>`，元组输出为 `<tuple> <elem> Hello </ elem> <elem> World </ ELEM> ... </tuple>` 。

## CapnProto {#capnproto}

Cap’n Proto 是一种二进制消息格式，类似 Protocol Buffers 和 Thriftis，但与 JSON 或 MessagePack 格式不一样。

Cap’n Proto 消息格式是严格类型的，而不是自我描述，这意味着它们不需要外部的描述。这种格式可以实时地应用，并针对每个查询进行缓存。

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits
       GROUP BY SearchPhrase FORMAT CapnProto SETTINGS schema = 'schema:Message'
```

其中 `schema.capnp` 描述如下：

    struct Message {
      SearchPhrase @0 :Text;
      c @1 :Uint64;
    }

格式文件存储的目录可以在服务配置中的 [format\_schema\_path](../operations/server_settings/settings.md) 指定。

Cap’n Proto 反序列化是很高效的，通常不会增加系统的负载。

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
Nested messages are suitable to input or output a [nested data structures](../data_types/nested_data_structures/nested.md).

Default values defined in a protobuf schema like this

``` capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

are not applied; the [table defaults](../query_language/create.md#create-default-values) are used instead of them.

ClickHouse inputs and outputs protobuf messages in the `length-delimited` format.
It means before every message should be written its length as a [varint](https://developers.google.com/protocol-buffers/docs/encoding#varints).
See also [how to read/write length-delimited protobuf messages in popular languages](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

## Avro {#data-format-avro}

[Apache Avro](http://avro.apache.org/) is a row-oriented data serialization framework developed within Apache’s Hadoop project.

ClickHouse Avro format supports reading and writing [Avro data files](http://avro.apache.org/docs/current/spec.html#Object+Container+Files).

### Data Types Matching {#data-types-matching}

The table below shows supported data types and how they match ClickHouse [data types](../data_types/index.md) in `INSERT` and `SELECT` queries.

| Avro data type `INSERT`                     | ClickHouse data type                                                                      | Avro data type `SELECT`      |
|---------------------------------------------|-------------------------------------------------------------------------------------------|------------------------------|
| `boolean`, `int`, `long`, `float`, `double` | [Int(8\|16\|32)](../data_types/int_uint.md), [UInt(8\|16\|32)](../data_types/int_uint.md) | `int`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](../data_types/int_uint.md), [UInt64](../data_types/int_uint.md)                   | `long`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](../data_types/float.md)                                                         | `float`                      |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](../data_types/float.md)                                                         | `double`                     |
| `bytes`, `string`, `fixed`, `enum`          | [String](../data_types/string.md)                                                         | `bytes`                      |
| `bytes`, `string`, `fixed`                  | [FixedString(N)](../data_types/fixedstring.md)                                            | `fixed(N)`                   |
| `enum`                                      | [Enum(8\|16)](../data_types/enum.md)                                                      | `enum`                       |
| `array(T)`                                  | [Array(T)](../data_types/array.md)                                                        | `array(T)`                   |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](../data_types/date.md)                                                      | `union(null, T)`             |
| `null`                                      | [Nullable(Nothing)](../data_types/special_data_types/nothing.md)                          | `null`                       |
| `int (date)` \*                             | [Date](../data_types/date.md)                                                             | `int (date)` \*              |
| `long (timestamp-millis)` \*                | [DateTime64(3)](../data_types/datetime.md)                                                | `long (timestamp-millis)` \* |
| `long (timestamp-micros)` \*                | [DateTime64(6)](../data_types/datetime.md)                                                | `long (timestamp-micros)` \* |

\* [Avro logical types](http://avro.apache.org/docs/current/spec.html#Logical+Types)

Unsupported Avro data types: `record` (non-root), `map`

Unsupported Avro logical data types: `uuid`, `time-millis`, `time-micros`, `duration`

### Inserting Data {#inserting-data}

To insert data from an Avro file into ClickHouse table:

``` bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

The root schema of input Avro file must be of `record` type.

To find the correspondence between table columns and fields of Avro schema ClickHouse compares their names. This comparison is case-sensitive.
Unused fields are skipped.

Data types of a ClickHouse table columns can differ from the corresponding fields of the Avro data inserted. When inserting data, ClickHouse interprets data types according to the table above and then [casts](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) the data to corresponding column type.

### Selecting Data {#selecting-data}

To select data from ClickHouse table into an Avro file:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

Column names must:

-   start with `[A-Za-z_]`
-   subsequently contain only `[A-Za-z0-9_]`

Output Avro file compression and sync interval can be configured with [output\_format\_avro\_codec](../operations/settings/settings.md#settings-output_format_avro_codec) and [output\_format\_avro\_sync\_interval](../operations/settings/settings.md#settings-output_format_avro_sync_interval) respectively.

## AvroConfluent {#data-format-avro-confluent}

AvroConfluent supports decoding single-object Avro messages commonly used with [Kafka](https://kafka.apache.org/) and [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html).

Each Avro message embeds a schema id that can be resolved to the actual schema with help of the Schema Registry.

Schemas are cached once resolved.

Schema Registry URL is configured with [format\_avro\_schema\_registry\_url](../operations/settings/settings.md#settings-format_avro_schema_registry_url)

### Data Types Matching {#data-types-matching-1}

Same as [Avro](#data-format-avro)

### Usage {#usage}

To quickly verify schema resolution you can use [kafkacat](https://github.com/edenhill/kafkacat) with [clickhouse-local](../operations/utils/clickhouse-local.md):

``` bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```

To use `AvroConfluent` with [Kafka](../operations/table_engines/kafka.md):

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
    Setting `format_avro_schema_registry_url` needs to be configured in `users.xml` to maintain it’s value after a restart.

## Parquet {#data-format-parquet}

[Apache Parquet](http://parquet.apache.org/) is a columnar storage format widespread in the Hadoop ecosystem. ClickHouse supports read and write operations for this format.

### Data Types Matching {#data-types-matching-2}

The table below shows supported data types and how they match ClickHouse [data types](../data_types/index.md) in `INSERT` and `SELECT` queries.

| Parquet data type (`INSERT`) | ClickHouse data type                        | Parquet data type (`SELECT`) |
|------------------------------|---------------------------------------------|------------------------------|
| `UINT8`, `BOOL`              | [UInt8](../data_types/int_uint.md)          | `UINT8`                      |
| `INT8`                       | [Int8](../data_types/int_uint.md)           | `INT8`                       |
| `UINT16`                     | [UInt16](../data_types/int_uint.md)         | `UINT16`                     |
| `INT16`                      | [Int16](../data_types/int_uint.md)          | `INT16`                      |
| `UINT32`                     | [UInt32](../data_types/int_uint.md)         | `UINT32`                     |
| `INT32`                      | [Int32](../data_types/int_uint.md)          | `INT32`                      |
| `UINT64`                     | [UInt64](../data_types/int_uint.md)         | `UINT64`                     |
| `INT64`                      | [Int64](../data_types/int_uint.md)          | `INT64`                      |
| `FLOAT`, `HALF_FLOAT`        | [Float32](../data_types/float.md)           | `FLOAT`                      |
| `DOUBLE`                     | [Float64](../data_types/float.md)           | `DOUBLE`                     |
| `DATE32`                     | [Date](../data_types/date.md)               | `UINT16`                     |
| `DATE64`, `TIMESTAMP`        | [DateTime](../data_types/datetime.md)       | `UINT32`                     |
| `STRING`, `BINARY`           | [String](../data_types/string.md)           | `STRING`                     |
| —                            | [FixedString](../data_types/fixedstring.md) | `STRING`                     |
| `DECIMAL`                    | [Decimal](../data_types/decimal.md)         | `DECIMAL`                    |

ClickHouse supports configurable precision of `Decimal` type. The `INSERT` query treats the Parquet `DECIMAL` type as the ClickHouse `Decimal128` type.

Unsupported Parquet data types: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

Data types of a ClickHouse table columns can differ from the corresponding fields of the Parquet data inserted. When inserting data, ClickHouse interprets data types according to the table above and then [cast](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) the data to that data type which is set for the ClickHouse table column.

### Inserting and Selecting Data {#inserting-and-selecting-data}

You can insert Parquet data from a file into ClickHouse table by the following command:

``` bash
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT Parquet"
```

You can select data from a ClickHouse table and save them into some file in the Parquet format by the following command:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Parquet" > {some_file.pq}
```

To exchange data with Hadoop, you can use [HDFS table engine](../operations/table_engines/hdfs.md).

## ORC {#data-format-orc}

[Apache ORC](https://orc.apache.org/) is a columnar storage format widespread in the Hadoop ecosystem. You can only insert data in this format to ClickHouse.

### Data Types Matching {#data-types-matching-3}

The table below shows supported data types and how they match ClickHouse [data types](../data_types/index.md) in `INSERT` queries.

| ORC data type (`INSERT`) | ClickHouse data type                  |
|--------------------------|---------------------------------------|
| `UINT8`, `BOOL`          | [UInt8](../data_types/int_uint.md)    |
| `INT8`                   | [Int8](../data_types/int_uint.md)     |
| `UINT16`                 | [UInt16](../data_types/int_uint.md)   |
| `INT16`                  | [Int16](../data_types/int_uint.md)    |
| `UINT32`                 | [UInt32](../data_types/int_uint.md)   |
| `INT32`                  | [Int32](../data_types/int_uint.md)    |
| `UINT64`                 | [UInt64](../data_types/int_uint.md)   |
| `INT64`                  | [Int64](../data_types/int_uint.md)    |
| `FLOAT`, `HALF_FLOAT`    | [Float32](../data_types/float.md)     |
| `DOUBLE`                 | [Float64](../data_types/float.md)     |
| `DATE32`                 | [Date](../data_types/date.md)         |
| `DATE64`, `TIMESTAMP`    | [DateTime](../data_types/datetime.md) |
| `STRING`, `BINARY`       | [String](../data_types/string.md)     |
| `DECIMAL`                | [Decimal](../data_types/decimal.md)   |

ClickHouse supports configurable precision of the `Decimal` type. The `INSERT` query treats the ORC `DECIMAL` type as the ClickHouse `Decimal128` type.

Unsupported ORC data types: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

The data types of ClickHouse table columns don’t have to match the corresponding ORC data fields. When inserting data, ClickHouse interprets data types according to the table above and then [casts](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) the data to the data type set for the ClickHouse table column.

### Inserting Data {#inserting-data-1}

You can insert ORC data from a file into ClickHouse table by the following command:

``` bash
$ cat filename.orc | clickhouse-client --query="INSERT INTO some_table FORMAT ORC"
```

To exchange data with Hadoop, you can use [HDFS table engine](../operations/table_engines/hdfs.md).

## Format Schema {#formatschema}

The file name containing the format schema is set by the setting `format_schema`.
It’s required to set this setting when it is used one of the formats `Cap'n Proto` and `Protobuf`.
The format schema is a combination of a file name and the name of a message type in this file, delimited by colon,
e.g. `schemafile.proto:MessageType`.
If the file has the standard extension for the format (for example, `.proto` for `Protobuf`),
it can be omitted and in this case the format schema looks like `schemafile:MessageType`.

If you input or output data via the [client](../interfaces/cli.md) in the interactive mode, the file name specified in the format schema
can contain an absolute path or a path relative to the current directory on the client.
If you use the client in the batch mode, the path to the schema must be relative due to security reasons.

If you input or output data via the [HTTP interface](../interfaces/http.md) the file name specified in the format schema
should be located in the directory specified in [format\_schema\_path](../operations/server_settings/settings.md#server_settings-format_schema_path)
in the server configuration.

[Original article](https://clickhouse.tech/docs/en/interfaces/formats/) <!--hide-->

## Skipping Errors {#skippingerrors}

Some formats such as `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` and `Protobuf` can skip broken row if parsing error occurred and continue parsing from the beginning of next row. See [input\_format\_allow\_errors\_num](../operations/settings/settings.md#settings-input_format_allow_errors_num) and
[input\_format\_allow\_errors\_ratio](../operations/settings/settings.md#settings-input_format_allow_errors_ratio) settings.
Limitations:
- In case of parsing error `JSONEachRow` skips all data until the new line (or EOF), so rows must be delimited by `\n` to count errors correctly.
- `Template` and `CustomSeparated` use delimiter after the last column and delimiter between rows to find the beginning of next row, so skipping errors works only if at least one of them is not empty.

[来源文章](https://clickhouse.tech/docs/zh/interfaces/formats/) <!--hide-->
