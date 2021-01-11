# 输入输出格式 {#formats}

ClickHouse 可以接受多种数据格式，可以在 (`INSERT`) 以及 (`SELECT`) 请求中使用。

下列表格列出了支持的数据格式以及在 (`INSERT`) 以及 (`SELECT`) 请求中使用它们的方式。

| 格式                                                            | INSERT | SELECT |
|-----------------------------------------------------------------|--------|--------|
| [TabSeparated](#tabseparated)                                   | ✔      | ✔      |
| [TabSeparatedRaw](#tabseparatedraw)                             | ✗      | ✔      |
| [TabSeparatedWithNames](#tabseparatedwithnames)                 | ✔      | ✔      |
| [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes) | ✔      | ✔      |
| [模板](#format-template)                                        | ✔      | ✔      |
| [TemplateIgnoreSpaces](#templateignorespaces)                   | ✔      | ✗      |
| [CSV](#csv)                                                     | ✔      | ✔      |
| [CSVWithNames](#csvwithnames)                                   | ✔      | ✔      |
| [自定义分离](#format-customseparated)                           | ✔      | ✔      |
| [值](#data-format-values)                                       | ✔      | ✔      |
| [垂直](#vertical)                                               | ✗      | ✔      |
| VerticalRaw                                                     | ✗      | ✔      |
| [JSON](#json)                                                   | ✗      | ✔      |
| [JSONCompact](#jsoncompact)                                     | ✗      | ✔      |
| [JSONEachRow](#jsoneachrow)                                     | ✔      | ✔      |
| [TSKV](#tskv)                                                   | ✔      | ✔      |
| [漂亮](#pretty)                                                 | ✗      | ✔      |
| [PrettyCompact](#prettycompact)                                 | ✗      | ✔      |
| [PrettyCompactMonoBlock](#prettycompactmonoblock)               | ✗      | ✔      |
| [PrettyNoEscapes](#prettynoescapes)                             | ✗      | ✔      |
| [PrettySpace](#prettyspace)                                     | ✗      | ✔      |
| [Protobuf](#protobuf)                                           | ✔      | ✔      |
| [Avro](#data-format-avro)                                       | ✔      | ✔      |
| [AvroConfluent](#data-format-avro-confluent)                    | ✔      | ✗      |
| [镶木地板](#data-format-parquet)                                | ✔      | ✔      |
| [ORC](#data-format-orc)                                         | ✔      | ✗      |
| [RowBinary](#rowbinary)                                         | ✔      | ✔      |
| [RowBinaryWithNamesAndTypes](#rowbinarywithnamesandtypes)       | ✔      | ✔      |
| [本地人](#native)                                               | ✔      | ✔      |
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

    1970-01-01      8873898

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

[NULL](../sql-reference/syntax.md) 将输出为 `\N`。

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

## 模板 {#format-template}

此格式允许为具有指定转义规则的值指定带有占位符的自定义格式字符串。

它使用设置 `format_schema`, `format_schema_rows`, `format_schema_rows_between_delimiter` and some settings of other formats (e.g. `output_format_json_quote_64bit_integers` 使用时 `JSON` 逃跑，进一步查看)

格式字符串 `format_schema_rows` 使用以下语法指定行格式:

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

该 `format_schema_rows_between_delimiter` setting指定行之间的分隔符，该分隔符在除最后一行之外的每一行之后打印（或预期） (`\n` 默认情况下)

格式字符串 `format_schema` 具有相同的语法 `format_schema_rows` 并允许指定前缀，后缀和打印一些附加信息的方式。 它包含以下占位符而不是列名:

-   `data` 包含数据的行 `format_schema_rows` 格式，由分隔 `format_schema_rows_between_delimiter`. 此占位符必须是格式字符串中的第一个占位符。
-   `totals` 是包含总值的行 `format_schema_rows` 格式（与总计一起使用时)
-   `min` 是具有最小值的行 `format_schema_rows` 格式（当极值设置为1时)
-   `max` 是具有最大值的行 `format_schema_rows` 格式（当极值设置为1时)
-   `rows` 输出行总数
-   `rows_before_limit` 是没有限制的最小行数。 仅当查询包含LIMIT时输出。 如果查询包含GROUP BY，则rows\_before\_limit\_at\_least是没有限制的确切行数。
-   `time` 请求执行时间以秒为单位
-   `rows_read` 已读取的行数
-   `bytes_read` 被读取的字节数（未压缩）

占位符 `data`, `totals`, `min` 和 `max` 必须没有指定转义规则（或 `None` 必须明确指定）。 其余的占位符可能具有指定的任何转义规则。
如果 `format_schema` 设置为空字符串, `${data}` 用作默认值。
对于插入查询格式允许跳过一些列或一些字段，如果前缀或后缀（见示例）。

`Select` 示例:

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

`Insert` 示例:

    Some header
    Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
    Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
    Total rows: 2

``` sql
INSERT INTO UserActivity FORMAT Template SETTINGS
format_schema = 'Some header\n${data}\nTotal rows: ${:CSV}\n',
format_schema_rows = 'Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}'
```

`PageViews`, `UserID`, `Duration` 和 `Sign` 占位符内部是表中列的名称。 值后 `Useless field` 在行和之后 `\nTotal rows:` in后缀将被忽略。
输入数据中的所有分隔符必须严格等于指定格式字符串中的分隔符。

## TemplateIgnoreSpaces {#templateignorespaces}

此格式仅适用于输入。
类似于 `Template`，但跳过输入流中的分隔符和值之间的空格字符。 但是，如果格式字符串包含空格字符，则在输入流中将需要这些字符。 还允许指定空占位符 (`${}` 或 `${:None}`）将一些分隔符分成单独的部分，以忽略它们之间的空格。 此类占位符仅用于跳过空格字符。
可以阅读 `JSON` 如果列的值在所有行中具有相同的顺序，则使用此格式。 例如，以下请求可用于从格式的输出示例中插入数据 [JSON](#json):

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

[NULL](../sql-reference/syntax.md) 输出为 `\N`。

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

## 自定义分离 {#format-customseparated}

类似于 [模板](#format-template)，但它打印或读取所有列，并使用从设置转义规则 `format_custom_escaping_rule` 从设置和分隔符 `format_custom_field_delimiter`, `format_custom_row_before_delimiter`, `format_custom_row_after_delimiter`, `format_custom_row_between_delimiter`, `format_custom_result_before_delimiter` 和 `format_custom_result_after_delimiter`，而不是从格式字符串。
也有 `CustomSeparatedIgnoreSpaces` 格式，这是类似于 `TemplateIgnoreSpaces`.

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

ClickHouse 支持 [NULL](../sql-reference/syntax.md), 在 JSON 格式中以 `null` 输出来表示.

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

### 嵌套结构的使用 {#jsoneachrow-nested}

如果你有一张桌子 [嵌套式](../sql-reference/data-types/nested-data-structures/nested.md) 数据类型列，可以插入具有相同结构的JSON数据。 启用此功能与 [input\_format\_import\_nested\_json](../operations/settings/settings.md#settings-input_format_import_nested_json) 设置。

例如，请考虑下表:

``` sql
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

正如你可以在找到 `Nested` 数据类型说明，ClickHouse将嵌套结构的每个组件视为单独的列, `n.s` 和 `n.i` 为了我们的桌子 所以你可以通过以下方式插入数据:

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

将数据作为分层JSON对象集插入 [input\_format\_import\_nested\_json=1](../operations/settings/settings.md#settings-input_format_import_nested_json).

``` json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

如果没有此设置，ClickHouse将引发异常。

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

## 本地人 {#native}

最高性能的格式。 据通过二进制格式的块进行写入和读取。对于每个块，该块中的行数，列数，列名称和类型以及列的部分将被相继记录。 换句话说，这种格式是 «列式»的 - 它不会将列转换为行。 这是用于在服务器之间进行交互的本地界面中使用的格式，用于使用命令行客户端和 C++ 客户端。

您可以使用此格式快速生成只能由 ClickHouse DBMS 读取的格式。但自己处理这种格式是没有意义的。

## Null {#null}

没有输出。但是，查询已处理完毕，并且在使用命令行客户端时，数据将传输到客户端。这仅用于测试，包括生产力测试。
显然，这种格式只适用于输出，不适用于解析。

## 漂亮 {#pretty}

将数据以表格形式输出，也可以使用 ANSI 转义字符在终端中设置颜色。
它会绘制一个完整的表格，每行数据在终端中占用两行。
每一个结果块都会以单独的表格输出。这是很有必要的，以便结果块不用缓冲结果输出（缓冲在可以预见结果集宽度的时候是很有必要的）。

[NULL](../sql-reference/syntax.md) 输出为 `ᴺᵁᴸᴸ`。

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
    │ 1970-01-01 │ 8873898 │
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

对于 [NULL](../sql-reference/syntax.md#null-literal) 的支持， 一个为 1 或 0 的字节会加在每个 [可为空](../sql-reference/data-types/nullable.md) 值前面。如果为 1, 那么该值就是 `NULL`。 如果为 0，则不为 `NULL`。

## RowBinaryWithNamesAndTypes {#rowbinarywithnamesandtypes}

类似于 [RowBinary](#rowbinary)，但添加了标题:

-   [LEB128](https://en.wikipedia.org/wiki/LEB128)-编码列数（N)
-   N `String`s指定列名
-   N `String`s指定列类型

## 值 {#data-format-values}

在括号中打印每一行。行由逗号分隔。最后一行之后没有逗号。括号内的值也用逗号分隔。数字以十进制格式输出，不含引号。 数组以方括号输出。带有时间的字符串，日期和时间用引号包围输出。转义字符的解析规则与 [TabSeparated](#tabseparated) 格式类似。 在格式化过程中，不插入额外的空格，但在解析过程中，空格是被允许并跳过的（除了数组值之外的空格，这是不允许的）。[NULL](../sql-reference/syntax.md) 为 `NULL`。

以 Values 格式传递数据时需要转义的最小字符集是：单引号和反斜线。

这是 `INSERT INTO t VALUES ...` 中可以使用的格式，但您也可以将其用于查询结果。

## 垂直 {#vertical}

使用指定的列名在单独的行上打印每个值。如果每行都包含大量列，则此格式便于打印一行或几行。

[NULL](../sql-reference/syntax.md) 输出为 `ᴺᵁᴸᴸ`。

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

格式文件存储的目录可以在服务配置中的 [format\_schema\_path](../operations/server-configuration-parameters/settings.md) 指定。

Cap’n Proto 反序列化是很高效的，通常不会增加系统的负载。

## Protobuf {#protobuf}

Protobuf-是一个 [协议缓冲区](https://developers.google.com/protocol-buffers/) 格式。

此格式需要外部格式架构。 在查询之间缓存架构。
ClickHouse支持 `proto2` 和 `proto3` 语法 支持重复/可选/必填字段。

使用示例:

``` sql
SELECT * FROM test.table FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

``` bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT Protobuf SETTINGS format_schema='schemafile:MessageType'"
```

哪里的文件 `schemafile.proto` 看起来像这样:

``` capnp
syntax = "proto3";

message MessageType {
  string name = 1;
  string surname = 2;
  uint32 birthDate = 3;
  repeated string phoneNumbers = 4;
};
```

要查找协议缓冲区的消息类型的表列和字段之间的对应关系，ClickHouse比较它们的名称。
这种比较是不区分大小写和字符 `_` (下划线)和 `.` （点）被认为是相等的。
如果协议缓冲区消息的列和字段的类型不同，则应用必要的转换。

支持嵌套消息。 例如，对于字段 `z` 在下面的消息类型

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

ClickHouse尝试找到一个名为 `x.y.z` （或 `x_y_z` 或 `X.y_Z` 等）。
嵌套消息适用于输入或输出一个 [嵌套数据结构](../sql-reference/data-types/nested-data-structures/nested.md).

在protobuf模式中定义的默认值，如下所示

``` capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

不应用;该 [表默认值](../sql-reference/statements/create.md#create-default-values) 用来代替它们。

ClickHouse在输入和输出protobuf消息 `length-delimited` 格式。
这意味着每个消息之前，应该写它的长度作为一个 [varint](https://developers.google.com/protocol-buffers/docs/encoding#varints).
另请参阅 [如何在流行语言中读取/写入长度分隔的protobuf消息](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

## Avro {#data-format-avro}

[Apache Avro](http://avro.apache.org/) 是在Apache Hadoop项目中开发的面向行的数据序列化框架。

ClickHouse Avro格式支持读取和写入 [Avro数据文件](http://avro.apache.org/docs/current/spec.html#Object+Container+Files).

### 数据类型匹配{\#sql\_reference/data\_types-matching} {#data-types-matching-sql_referencedata_types-matching}

下表显示了支持的数据类型以及它们如何匹配ClickHouse [数据类型](../sql-reference/data-types/index.md) 在 `INSERT` 和 `SELECT` 查询。

| Avro数据类型 `INSERT`                       | ClickHouse数据类型                                                                                                | Avro数据类型 `SELECT`        |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------|------------------------------|
| `boolean`, `int`, `long`, `float`, `double` | [Int(8/16/32)](../sql-reference/data-types/int-uint.md), [UInt(8/16/32)](../sql-reference/data-types/int-uint.md) | `int`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](../sql-reference/data-types/int-uint.md), [UInt64](../sql-reference/data-types/int-uint.md)               | `long`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](../sql-reference/data-types/float.md)                                                                   | `float`                      |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](../sql-reference/data-types/float.md)                                                                   | `double`                     |
| `bytes`, `string`, `fixed`, `enum`          | [字符串](../sql-reference/data-types/string.md)                                                                   | `bytes`                      |
| `bytes`, `string`, `fixed`                  | [固定字符串(N)](../sql-reference/data-types/fixedstring.md)                                                       | `fixed(N)`                   |
| `enum`                                      | [枚举(8/16)](../sql-reference/data-types/enum.md)                                                                 | `enum`                       |
| `array(T)`                                  | [阵列(T)](../sql-reference/data-types/array.md)                                                                   | `array(T)`                   |
| `union(null, T)`, `union(T, null)`          | [可为空(T)](../sql-reference/data-types/date.md)                                                                  | `union(null, T)`             |
| `null`                                      | [可为空（无)](../sql-reference/data-types/special-data-types/nothing.md)                                          | `null`                       |
| `int (date)` \*                             | [日期](../sql-reference/data-types/date.md)                                                                       | `int (date)` \*              |
| `long (timestamp-millis)` \*                | [DateTime64(3)](../sql-reference/data-types/datetime.md)                                                          | `long (timestamp-millis)` \* |
| `long (timestamp-micros)` \*                | [DateTime64(6)](../sql-reference/data-types/datetime.md)                                                          | `long (timestamp-micros)` \* |

\* [Avro逻辑类型](http://avro.apache.org/docs/current/spec.html#Logical+Types)

不支持的Avro数据类型: `record` （非根), `map`

不支持的Avro逻辑数据类型: `uuid`, `time-millis`, `time-micros`, `duration`

### 插入数据 {#inserting-data}

将Avro文件中的数据插入ClickHouse表:

``` bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

输入Avro文件的根模式必须是 `record` 类型。

要查找Avro schema的表列和字段之间的对应关系，ClickHouse比较它们的名称。 此比较区分大小写。
跳过未使用的字段。

ClickHouse表列的数据类型可能与插入的Avro数据的相应字段不同。 插入数据时，ClickHouse根据上表解释数据类型，然后 [投](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) 将数据转换为相应的列类型。

### 选择数据 {#selecting-data}

从ClickHouse表中选择数据到Avro文件:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

列名必须:

-   名,名,名,名 `[A-Za-z_]`
-   随后只包含 `[A-Za-z0-9_]`

输出Avro文件压缩和同步间隔可以配置 [output\_format\_avro\_codec](../operations/settings/settings.md#settings-output_format_avro_codec) 和 [output\_format\_avro\_sync\_interval](../operations/settings/settings.md#settings-output_format_avro_sync_interval) 分别。

## AvroConfluent {#data-format-avro-confluent}

AvroConfluent支持解码单对象Avro消息常用于 [卡夫卡](https://kafka.apache.org/) 和 [汇合的模式注册表](https://docs.confluent.io/current/schema-registry/index.html).

每个Avro消息都嵌入了一个架构id，该架构id可以在架构注册表的帮助下解析为实际架构。

模式解析后会进行缓存。

架构注册表URL配置为 [format\_avro\_schema\_registry\_url](../operations/settings/settings.md#settings-format_avro_schema_registry_url)

### 数据类型匹配{\#sql\_reference/data\_types-matching-1} {#data-types-matching-sql_referencedata_types-matching-1}

和 [Avro](#data-format-avro)

### 用途 {#usage}

要快速验证架构解析，您可以使用 [kafkacat](https://github.com/edenhill/kafkacat) 与 [ﾂ环板-ｮﾂ嘉ｯﾂ偲](../operations/utilities/clickhouse-local.md):

``` bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```

使用 `AvroConfluent` 与 [卡夫卡](../engines/table-engines/integrations/kafka.md):

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

!!! note "警告"
    设置 `format_avro_schema_registry_url` 需要在配置 `users.xml` restart动后保持它的价值。

## 镶木地板 {#data-format-parquet}

[阿帕奇地板](http://parquet.apache.org/) 是Hadoop生态系统中普遍存在的列式存储格式。 ClickHouse支持此格式的读写操作。

### 数据类型匹配{\#sql\_reference/data\_types-matching-2} {#data-types-matching-sql_referencedata_types-matching-2}

下表显示了支持的数据类型以及它们如何匹配ClickHouse [数据类型](../sql-reference/data-types/index.md) 在 `INSERT` 和 `SELECT` 查询。

| Parquet数据类型 (`INSERT`) | ClickHouse数据类型                                       | Parquet数据类型 (`SELECT`) |
|----------------------------|----------------------------------------------------------|----------------------------|
| `UINT8`, `BOOL`            | [UInt8](../sql-reference/data-types/int-uint.md)         | `UINT8`                    |
| `INT8`                     | [Int8](../sql-reference/data-types/int-uint.md)          | `INT8`                     |
| `UINT16`                   | [UInt16](../sql-reference/data-types/int-uint.md)        | `UINT16`                   |
| `INT16`                    | [Int16](../sql-reference/data-types/int-uint.md)         | `INT16`                    |
| `UINT32`                   | [UInt32](../sql-reference/data-types/int-uint.md)        | `UINT32`                   |
| `INT32`                    | [Int32](../sql-reference/data-types/int-uint.md)         | `INT32`                    |
| `UINT64`                   | [UInt64](../sql-reference/data-types/int-uint.md)        | `UINT64`                   |
| `INT64`                    | [Int64](../sql-reference/data-types/int-uint.md)         | `INT64`                    |
| `FLOAT`, `HALF_FLOAT`      | [Float32](../sql-reference/data-types/float.md)          | `FLOAT`                    |
| `DOUBLE`                   | [Float64](../sql-reference/data-types/float.md)          | `DOUBLE`                   |
| `DATE32`                   | [日期](../sql-reference/data-types/date.md)              | `UINT16`                   |
| `DATE64`, `TIMESTAMP`      | [日期时间](../sql-reference/data-types/datetime.md)      | `UINT32`                   |
| `STRING`, `BINARY`         | [字符串](../sql-reference/data-types/string.md)          | `STRING`                   |
| —                          | [固定字符串](../sql-reference/data-types/fixedstring.md) | `STRING`                   |
| `DECIMAL`                  | [十进制](../sql-reference/data-types/decimal.md)         | `DECIMAL`                  |

ClickHouse支持可配置的精度 `Decimal` 类型。 该 `INSERT` 查询对待实木复合地板 `DECIMAL` 键入为ClickHouse `Decimal128` 类型。

不支持的Parquet数据类型: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

ClickHouse表列的数据类型可能与插入的Parquet数据的相应字段不同。 插入数据时，ClickHouse根据上表解释数据类型，然后 [投](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) 为ClickHouse表列设置的数据类型的数据。

### 插入和选择数据 {#inserting-and-selecting-data}

您可以通过以下命令将Parquet数据从文件插入到ClickHouse表中:

``` bash
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT Parquet"
```

您可以从ClickHouse表中选择数据，并通过以下命令将它们保存到Parquet格式的某个文件中:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Parquet" > {some_file.pq}
```

要与Hadoop交换数据，您可以使用 [HDFS表引擎](../engines/table-engines/integrations/hdfs.md).

## ORC {#data-format-orc}

[阿帕奇兽人](https://orc.apache.org/) 是Hadoop生态系统中普遍存在的列式存储格式。 您只能将此格式的数据插入ClickHouse。

### 数据类型匹配{\#sql\_reference/data\_types-matching-3} {#data-types-matching-sql_referencedata_types-matching-3}

下表显示了支持的数据类型以及它们如何匹配ClickHouse [数据类型](../sql-reference/data-types/index.md) 在 `INSERT` 查询。

| ORC数据类型 (`INSERT`) | ClickHouse数据类型                                  |
|------------------------|-----------------------------------------------------|
| `UINT8`, `BOOL`        | [UInt8](../sql-reference/data-types/int-uint.md)    |
| `INT8`                 | [Int8](../sql-reference/data-types/int-uint.md)     |
| `UINT16`               | [UInt16](../sql-reference/data-types/int-uint.md)   |
| `INT16`                | [Int16](../sql-reference/data-types/int-uint.md)    |
| `UINT32`               | [UInt32](../sql-reference/data-types/int-uint.md)   |
| `INT32`                | [Int32](../sql-reference/data-types/int-uint.md)    |
| `UINT64`               | [UInt64](../sql-reference/data-types/int-uint.md)   |
| `INT64`                | [Int64](../sql-reference/data-types/int-uint.md)    |
| `FLOAT`, `HALF_FLOAT`  | [Float32](../sql-reference/data-types/float.md)     |
| `DOUBLE`               | [Float64](../sql-reference/data-types/float.md)     |
| `DATE32`               | [日期](../sql-reference/data-types/date.md)         |
| `DATE64`, `TIMESTAMP`  | [日期时间](../sql-reference/data-types/datetime.md) |
| `STRING`, `BINARY`     | [字符串](../sql-reference/data-types/string.md)     |
| `DECIMAL`              | [十进制](../sql-reference/data-types/decimal.md)    |

ClickHouse支持的可配置精度 `Decimal` 类型。 该 `INSERT` 查询对待兽人 `DECIMAL` 键入为ClickHouse `Decimal128` 类型。

不支持的ORC数据类型: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

ClickHouse表列的数据类型不必匹配相应的ORC数据字段。 插入数据时，ClickHouse根据上表解释数据类型，然后 [投](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) 将数据转换为ClickHouse表列的数据类型集。

### 插入数据 {#inserting-data-1}

您可以通过以下命令将文件中的ORC数据插入到ClickHouse表中:

``` bash
$ cat filename.orc | clickhouse-client --query="INSERT INTO some_table FORMAT ORC"
```

要与Hadoop交换数据，您可以使用 [HDFS表引擎](../engines/table-engines/integrations/hdfs.md).

## 格式架构 {#formatschema}

包含格式架构的文件名由该设置设置 `format_schema`.
当使用其中一种格式时，需要设置此设置 `Cap'n Proto` 和 `Protobuf`.
格式架构是文件名和此文件中消息类型的名称的组合，用冒号分隔,
e.g. `schemafile.proto:MessageType`.
如果文件具有格式的标准扩展名（例如, `.proto` 为 `Protobuf`),
它可以被省略，在这种情况下，格式模式如下所示 `schemafile:MessageType`.

如果您通过输入或输出数据 [客户](../interfaces/cli.md) 在交互模式下，格式架构中指定的文件名
可以包含绝对路径或相对于客户端上当前目录的路径。
如果在批处理模式下使用客户端，则由于安全原因，架构的路径必须是相对的。

如果您通过输入或输出数据 [HTTP接口](../interfaces/http.md) 格式架构中指定的文件名
应该位于指定的目录中 [format\_schema\_path](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-format_schema_path)
在服务器配置中。

[原始文章](https://clickhouse.tech/docs/en/interfaces/formats/) <!--hide-->

## 跳过错误 {#skippingerrors}

一些格式，如 `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` 和 `Protobuf` 如果发生解析错误，可以跳过断开的行，并从下一行开始继续解析。 看 [input\_format\_allow\_errors\_num](../operations/settings/settings.md#settings-input_format_allow_errors_num) 和
[input\_format\_allow\_errors\_ratio](../operations/settings/settings.md#settings-input_format_allow_errors_ratio) 设置。
限制:
-在解析错误的情况下 `JSONEachRow` 跳过所有数据，直到新行（或EOF），所以行必须由 `\n` 正确计算错误。
- `Template` 和 `CustomSeparated` 在最后一列之后使用分隔符，并在行之间使用分隔符来查找下一行的开头，所以跳过错误只有在其中至少有一个不为空时才有效。

[来源文章](https://clickhouse.tech/docs/zh/interfaces/formats/) <!--hide-->
