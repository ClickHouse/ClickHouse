---
toc_priority: 21
toc_title: 输入/输出格式
---

# 输入/输出格式 {#formats}

ClickHouse可以接受和返回各种格式的数据。受支持的输入格式可用于提交给`INSERT`语句、从文件表(File,URL,HDFS或者外部目录)执行`SELECT`语句，受支持的输出格式可用于格式化`SELECT`语句的返回结果，或者通过`INSERT`写入到文件表。


以下是支持的格式:

| 格式                                                                                  | 输入 | 输出 |
|-----------------------------------------------------------------------------------------|-------|--------|
| [TabSeparated](#tabseparated)                                                           | ✔     | ✔      |
| [TabSeparatedRaw](#tabseparatedraw)                                                     | ✔     | ✔      |
| [TabSeparatedWithNames](#tabseparatedwithnames)                                         | ✔     | ✔      |
| [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes)                         | ✔     | ✔      |
| [Template](#format-template)                                                            | ✔     | ✔      |
| [TemplateIgnoreSpaces](#templateignorespaces)                                           | ✔     | ✗      |
| [CSV](#csv)                                                                             | ✔     | ✔      |
| [CSVWithNames](#csvwithnames)                                                           | ✔     | ✔      |
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
| [JSONCompactEachRowWithNamesAndTypes](#jsoncompacteachrowwithnamesandtypes)             | ✔     | ✔      |
| [JSONCompactStringsEachRow](#jsoncompactstringseachrow)                                   | ✔     | ✔      |
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
| [RowBinaryWithNamesAndTypes](#rowbinarywithnamesandtypes)                               | ✔     | ✔      |
| [Native](#native)                                                                       | ✔     | ✔      |
| [Null](#null)                                                                           | ✗     | ✔      |
| [XML](#xml)                                                                             | ✗     | ✔      |
| [CapnProto](#capnproto)                                                                 | ✔     | ✗      |
| [LineAsString](#lineasstring)                                                           | ✔     | ✗      |
| [Regexp](#data-format-regexp)                                                           | ✔     | ✗      |
| [RawBLOB](#rawblob)                                                                     | ✔     | ✔      |


您可以使用ClickHouse设置一些格式化参数。更多详情设置请参考[设置](../operations/settings/settings.md)

## TabSeparated {#tabseparated}

在TabSeparated分隔格式中，数据按行写入。每行包含由制表符分隔的值，每个值后跟一个制表符，除了行中最后一个值，最后的值后面是一个换行符。在任何地方都采用严格的Unix换行(\n)。最后一行结束后必须再插入一个换行符。值以文本格式编写，不包含引号，并使用转义的特殊字符。

这种格式也被称为`TSV`。

`TabSeparated`格式便于其他的程序和脚本处理数据。默认情况下，HTTP接口和命令行客户端的批处理模式中会使用这个格式。这种格式还允许在不同dbms之间传输数据。例如，您可以从MySQL获取转储并将其上传到ClickHouse，反之亦然。

`TabSeparated`格式支持输出总计的结果(当SQL语句包含`WITH TOTALS`)和极值(当`extremes`被设置为1时)。在这种情况下，总计值和极值会在主数据后输出。主要结果、总值和极值之间用空行分隔。示例:

``` sql
set extremes=1;
SELECT EventDate, count() AS c FROM test.hits_v1 GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated;
```

``` text
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

### 数据格式化 {#data-formatting}

整数是用十进制形式写的。数字可以在开头包含一个额外的`+`字符(解析时忽略该符号，格式化时不记录该符号)。非负数不能包含负号。在读取时，允许将空字符串解析为零，或者(对于有符号类型)将'-'(仅有减号的字符串)解析为零。不符合相应数据类型的数字可能被解析为数值，而不会出现错误消息。

浮点数以十进制形式书写。用`.`作为小数点的符号。支持指数符号，如`inf`、`+inf`、`-inf`和`nan`。小数点前或后可以不出现数字(如123.或.123)。
在格式化期间，浮点数精度可能会丢失。
在解析期间，并不严格要求读取与机器可以表示的最接近的数值。

日期以`YYYY-MM-DD`格式编写，并以相同的格式解析，但允许使用任何字符作为分隔符。
日期和时间以`YYYY-MM-DD hh:mm:ss`的格式书写，并以相同的格式解析，但允许使用任何字符作为分隔符。
时区采用客户端或服务器端时区(取决于谁对数据进行格式化)。对于带有时间的日期，没有是哦用夏时制时间。因此，如果导出的数据采用了夏时制，则实际入库的时间不一定与预期的时间对应，解析将根据解析动作发起方选择时间。
在读取操作期间，不正确的日期和具有时间的日期可以自然溢出(如2021-01-32)或设置成空日期和时间，而不会出现错误消息。

有个例外情况，时间解析也支持Unix时间戳(如果它恰好由10个十进制数字组成)。其结果与时区无关。格式`YYYY-MM-DD hh:mm:ss`和`NNNNNNNNNN`这两种格式会自动转换。

字符串输出时，特殊字符会自动转义。以下转义序列用于输出:`\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`。解析还支持`\a`、`\v`和`\xHH`(HH代表十六进制编码)和`\c`，其中`c`是任何字符(这些序列被转换为`c`)。因此，读取数据时，换行符可以写成`\n`或`\`。例如，如果要表示字符串`Hello world`中'Hello'与'world'中间的空格实际上是个换行符，可以写成下面的形式:

``` text
Hello\nworld
```
等同于
``` text
Hello\
world
```

第二种形式也受支持，因为MySQL导出tab-separated格式的数据时使用这种格式。

使用TabSeparated格式传递数据时至少需要转义以下特殊字符:制表符(\t)、换行符(\n)和反斜杠(\\)。

只有一小部分符号被转义。您可以很容易地找到一个能够破坏命令行终端输出的特殊字符。

数组用方括号包裹、逗号分隔的形式表示(例如`[11,22,33]`)。数组中的数字项按上述规则进行格式化。`日期`和`日期时间`类型用单引号包裹。字符串用单引号包裹，遵循上述转义规则。

[NULL](https://clickhouse.com/docs/zh/sql-reference/syntax/)将输出为`\N`。

[Nested](https://clickhouse.com/docs/zh/sql-reference/data-types/nested-data-structures/nested/)结构的每个元素都表示为数组。

示例：

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

与`TabSeparated`格式的不同之处在于，写入的数据不会进行转义处理。
使用这种格式解析时，每个字段中不允许出现制表符或换行符。

这种格式也被称为`TSVRaw`。

## TabSeparatedWithNames {#tabseparatedwithnames}

不同于`TabSeparated`，列名会写在第一行。
在解析过程中，第一行被完全忽略。您不能依赖列名来确定它们的位置或检查它们的正确性。
(将来可能会添加对头行解析的支持。)

这种格式也被称为`TSVWithNames`。

## TabSeparatedWithNamesAndTypes {#tabseparatedwithnamesandtypes}

与`TabSeparated`格式不同的是列名写在第一行，而列类型写在第二行。
在解析过程中，将完全忽略第一行和第二行。

这种格式也被称为`TSVWithNamesAndTypes`。

## Template {#format-template}

此格式允许指定带有占位符的自定义格式字符串，这些占位符用于指定转义规则。

它使用`format_schema`, `format_schema_rows`, `format_schema_rows_between_delimiter`以及其他格式的一些设置(例如转义`JSON`时使用`output_format_json_quote_64bit_integers`，具体请向下阅读)

设置`format_template_row`用于指定行格式文件的路径，该格式文件包含行格式字符串，语法如下:

`delimiter_i${column_i:serializeAs_i}delimiter_i${column_i:serializeAs_i} ... delimiter_i`,

其中，`delimiter_i`是各值之间的分隔符(`$`符号可以转义为`$$`)，
`column_i`是选择或插入值的列的名称或索引(如果为空，则跳过该列)，
`serializeAs_i`是列值的转义规则。支持以下转义规则:

-   `CSV`, `JSON`, `XML` (类似于相同名称的格式)
-   `Escaped` (类似于`TSV`)
-   `Quoted` (类似于`Values`)
-   `Raw` (不转义，类似于`TSVRaw`)
-   `None` (不转义，具体请向下阅读)

如果省略了转义规则，那么将使用`None`。`XML`和`Raw`只适用于输出。

对于下面的格式字符串:

      `Search phrase: ${SearchPhrase:Quoted}, count: ${c:Escaped}, ad price: $$${price:JSON};`

`SearchPhrase`、`c`和`price`列的值遵循`Quoted`、`Escaped`和`JSON`转义规则，将分别在`Search phrase:`， `， count: `， `， ad price: $`和`;`分隔符之间打印(用于`SELECT`)或输入期望的值(用于`INSERT`)。例如:

`Search phrase: 'bathroom interior design', count: 2166, ad price: $3;`

`format_template_rows_between_delimiter`设置指定行之间的分隔符，它将打印(或输入期望值)在每一行之后，最后一行除外(该设置默认值为`\n`)。

设置`format_template_resultset`指定结果集格式文件路径，该文件包含结果集的格式字符串。结果集的格式字符串与上述的行格式字符串具有相同的语法，并允许指定前缀、后缀，还提供打印一些附加信息的方法。该文件使用如下占位符，用于取代行格式字符串的列名的位置(即`column_i`):

-   `data` 代表遵循`format_template_row`格式的数据行，由`format_template_rows_between_delimiter`设置制定的字符分隔。此占位符必须是格式字符串中的第一个占位符。
-   `totals` 代表遵循`format_template_row`格式的数据行，该行用于代表结果的总计值(当SQL语句包含了`WITH TOTALS`)
-   `min` 代表遵循`format_template_row`格式的数据行，该行用于代表结果的最小值(当`extremes`设置为1时)
-   `max` 代表遵循`format_template_row`格式的数据行，该行用于代表结果的最大值(当`extremes`设置为1时)
-   `rows` 代表输出行的总数
-   `rows_before_limit` 代表没有LIMIT限制的结果最小行数。仅当查询包含LIMIT时才输出此值。如果查询包含GROUP BY，那么`rows_before_limit_at_least`就是没有LIMIT的确切行数。
-   `time` 代表请求执行时间(秒)
-   `rows_read` 代表已读取的行数
-   `bytes_read` 代表已读取(未压缩)的字节数

占位符`data`、`totals`、`min`和`max`不允许指定转义规则(允许显式指定`None`)。其余占位符可以指定任何转义规则。
如果`format_template_resultset`设置为空，则使用`${data}`作为默认值。
对于insert查询，格式允许跳过某些列或某些字段的前缀或后缀(参见示例)。

Select示例:

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

结果：

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

Insert示例：

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

`PageViews`, `UserID`, `Duration`和`Sign` 占位符是表中列的名称。将忽略行中`Useless field`后面和后缀中`\nTotal rows:`之后的值。
输入数据中的所有分隔符必须严格等于指定格式字符串中的分隔符。

## TemplateIgnoreSpaces {#templateignorespaces}

这种格式只适用于输入。
类似于`Template`，但跳过输入流中分隔符和值之间的空白字符。但是，如果格式字符串包含空格字符，这些字符将会出现在输入流中。也允许指定空占位符(`${}`或`${:None}`)来将一些分隔符分割为单独的部分，以忽略它们之间的空格。这种占位符仅用于跳过空白字符。
如果列的值在所有行的顺序相同，那么可以使用这种格式读取`JSON`。可以使用以下请求从格式为[JSON](#json)的输出示例中插入数据：

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

类似于TabSeparated，但是输出的值是name=value格式。名称的转义方式与TabSeparated格式相同，=符号也会被转义。

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

[NULL](../sql-reference/syntax.md)转化为`\N`。

``` sql
SELECT * FROM t_null FORMAT TSKV
```

``` text
x=1    y=\N
```

当有大量的小列时，这种格式效率十分低下，并且通常没有理由使用它。不过，就效率而言，它并不比JSONEachRow差。
这种格式支持数据输出和解析。用于解析时，可以任意指定列的顺序，也可以省略某些列，那些列的值为该列的默认值，一般情况下为0或空白。不支持将可在表中可指定的复杂值设为默认值。

解析时允许出现后没有=的字段`tskv`。此字段会被忽略。

## CSV {#csv}

按`,`分隔的数据格式([RFC](https://tools.ietf.org/html/rfc4180))。

格式化时，每一行的值会用双引号括起，日期和时间也会以双引号包括。数字不用双引号括起，字符串中的双引号会以两个双引号输出，除此之外没有其他规则来做字符转义了。值由分隔符隔开，这个分隔符默认是`,`。每一行使用Unix换行符(LF,\n)分隔。
数组序列化成CSV规则如下：首先将数组序列化为`TabSeparated`格式的字符串，然后将结果字符串用双引号括起后输出到`CSV`。`CSV`格式的元组被序列化为单独的列(即它们在元组中的嵌套关系会丢失)。

``` bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

\* 默认情况下分隔符是`,` ，在[format_csv_delimiter](../operations/settings/settings.md#settings-format_csv_delimiter)中可以了解更多分隔符配置。

解析的时候，值可以使用或不使用双引号或者单引号括起来。在这种情况下，每行通过分隔符或换行符(`CR`或`LF`)区分。违反`RFC`规则的是，在解析未用引号括起的行时，会忽略前缀和结尾的空格和制表符。对于换行符，Unix(LF,\n)，Windows(CR LF\r\n)和Mac OS Classic(CR LF\t\n)都受支持。

如果启用[input_format_defaults_for_omitted_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields)，对应列如果存在未输入的空白，且没有用双引号括起，将用默认值替换。

`NULL`被格式化为`\N`或`NULL`或一个不是引号的其他字符串(详见配置[input_format_csv_unquoted_null_literal_as_null](../operations/settings/settings.md#settings-input_format_csv_unquoted_null_literal_as_null)或[input_format_defaults_for_omitted_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields))。

`CSV`格式支持输出总数和极值的方式与`TabSeparated`相同。

## CSVWithNames {#csvwithnames}

会输出带头部的信息(字段列表)，和`TabSeparatedWithNames`一样。

## CustomSeparated {#format-customseparated}

类似于[Template](#format-template)， 但它打印或读取所有列，并使用设置`format_custom_escaping_rule`和分隔符设置`format_custom_field_delimiter`,`format_custom_row_before_delimiter`,`format_custom_row_after_delimiter`,`format_custom_row_between_delimiter`,`format_custom_result_before_delimiter`,`format_custom_result_after_delimiter`的转义规则,而不是从格式字符串。
也有`CustomSeparatedIgnoreSpaces`格式，这个类似于`TemplateIgnoreSpaces`。

## JSON {#json}

以JSON格式输出数据。除了数据表之外，它还输出列名和类型，以及一些附加信息: 输出行的总数，以及如果没有LIMIT的话可输出的行数。示例:

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

JSON与JavaScript兼容。为了确保这一点，一些字符被另外转义：斜线`/`被转义为`\/`; 换行符`U+2028`和`U+2029`会打断一些浏览器的解析，它们会被转义为`\uXXXX`。 ASCII控制字符被转义：退格，换页，换行，回车和制表符被转义为`\b`，`\f`，`\n`，`\r`，`\t`。剩下的0x00-0x1F被转义成相应的`\uXXXX`序列。 无效的UTF-8序列替换为字符�，使得输出文本包含有效的UTF-8序列。 为了与JavaScript兼容，默认情况下，Int64和UInt64整数用双引号引起来。要除去引号，可以将配置参数`output_format_json_quote_64bit_integers`设置为0。

`rows`代表结果输出的行数。

`rows_before_limit_at_least`代表去掉LIMIT过滤后的最小行总数。只会在查询包含LIMIT条件时输出。若查询包含 GROUP BY，`rows_before_limit_at_least`就是去掉LIMIT后过滤后的准确行数。

`totals` – 总值 (当指定`WITH TOTALS`时)。

`extremes` – 极值(当extremes设置为1时)。

该格式仅适用于输出查询结果，但不适用于解析输入(将数据插入到表中)。

ClickHouse支持[NULL](../sql-reference/syntax.md), 在JSON输出中显示为`null`。若要在输出中启用`+nan`、`-nan`、`+inf`、`-inf`值，请设置[output_format_json_quote_denormals](../operations/settings/settings.md#settings-output_format_json_quote_denormals)为1。

**参考**

-   [JSONEachRow](#jsoneachrow)格式
-   [output_format_json_array_of_rows](../operations/settings/settings.md#output-format-json-array-of-rows)设置

## JSONStrings {#jsonstrings}

与JSON的不同之处在于数据字段以字符串输出，而不是以类型化JSON值输出。

示例：

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
注意range(5)的值。

## JSONAsString {#jsonasstring}

在这种格式中，一个JSON对象被解释为一个值。如果输入有几个JSON对象(逗号分隔)，它们将被解释为独立的行。

这种格式只能针对有一列类型为[String](../sql-reference/data-types/string.md)的表。表中其余的列必须设置为[DEFAULT](../sql-reference/statements/create.md)或[MATERIALIZED](../sql-reference/statements/create.md)，或者忽略。一旦将整个JSON对象收集为字符串，就可以使用[JSON函数](../sql-reference/functions/json-functions.md)运行它。

**示例**

查询：

``` sql
DROP TABLE IF EXISTS json_as_string;
CREATE TABLE json_as_string (json String) ENGINE = Memory;
INSERT INTO json_as_string FORMAT JSONAsString {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_string;
```

结果：

``` text
┌─json──────────────────────────────┐
│ {"foo":{"bar":{"x":"y"},"baz":1}} │
│ {}                                │
│ {"any json stucture":1}           │
└───────────────────────────────────┘
```

## JSONCompact {#jsoncompact}
## JSONCompactStrings {#jsoncompactstrings}

与JSON格式不同的是它以数组的方式输出结果，而不是以结构体。

示例：

``` json
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

```json
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

使用这些格式时，ClickHouse会将行输出为用换行符分隔的JSON值，这些输出数据作为一个整体时，由于没有分隔符(,)因而不是有效的JSON文档。

``` json
{"some_int":42,"some_str":"hello","some_tuple":[1,"a"]} // JSONEachRow
[42,"hello",[1,"a"]] // JSONCompactEachRow
["42","hello","(2,'a')"] // JSONCompactStringsEachRow
```

在插入数据时，应该为每一行提供一个单独的JSON值。

## JSONEachRowWithProgress {#jsoneachrowwithprogress}
## JSONStringsEachRowWithProgress {#jsonstringseachrowwithprogress}

与`JSONEachRow`/`JSONStringsEachRow`不同的是，ClickHouse还将生成作为JSON值的进度信息。

```json
{"row":{"'hello'":"hello","multiply(42, number)":"0","range(5)":[0,1,2,3,4]}}
{"row":{"'hello'":"hello","multiply(42, number)":"42","range(5)":[0,1,2,3,4]}}
{"row":{"'hello'":"hello","multiply(42, number)":"84","range(5)":[0,1,2,3,4]}}
{"progress":{"read_rows":"3","read_bytes":"24","written_rows":"0","written_bytes":"0","total_rows_to_read":"3"}}
```

## JSONCompactEachRowWithNamesAndTypes {#jsoncompacteachrowwithnamesandtypes}
## JSONCompactStringsEachRowWithNamesAndTypes {#jsoncompactstringseachrowwithnamesandtypes}

与`JSONCompactEachRow`/`JSONCompactStringsEachRow`不同的是，列名和类型被写入前两行。

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

ClickHouse允许:

- 以任意顺序排列列名，后跟对应的值。
- 省略一些值。

ClickHouse忽略元素之间的空格和对象后面的逗号。您可以在一行中传递所有对象，不需要用换行符把它们分开。

**省略值处理**

ClickHouse将省略的值替换为对应的[数据类型](../sql-reference/data-types/index.md)默认值。

如果指定了`DEFAULT expr`，则ClickHouse根据属性使用不同的替换规则，详看[input_format_defaults_for_omitted_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields)设置。

参考下面的例子：

``` sql
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

-   如果`input_format_defaults_for_omitted_fields = 0`, 那么`x`和`a`的默认值等于`0`(作为`UInt32`数据类型的默认值)。
-   如果`input_format_defaults_for_omitted_fields = 1`, 那么`x`的默认值为`0`，但`a`的默认值为`x * 2`。

!!! note "注意"
当使用`input_format_defaults_for_omitted_fields = 1`插入数据时，与使用`input_format_defaults_for_omitted_fields = 0`相比，ClickHouse消耗更多的计算资源。

### Selecting Data {#selecting-data}

以`UserActivity`表为例:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

当查询`SELECT * FROM UserActivity FORMAT JSONEachRow`返回:

``` text
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

与[JSON](#json)格式不同，没有替换无效的UTF-8序列。值以与`JSON`相同的方式转义。

!!! note "提示"
字符串中可以输出任意一组字节。如果您确信表中的数据可以被格式化为JSON而不会丢失任何信息，那么就使用`JSONEachRow`格式。

### Nested Structures {#jsoneachrow-nested}

如果您有一个包含[Nested](../sql-reference/data-types/nested-data-structures/nested.md)数据类型列的表，您可以插入具有相同结构的JSON数据。使用[input_format_import_nested_json](../operations/settings/settings.md#settings-input_format_import_nested_json)设置启用该特性。

例如，请参考下表:

``` sql
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

正如您在`Nested`数据类型描述中看到的，ClickHouse将嵌套结构的每个部分作为一个单独的列(`n.s`和`n.i`)。您可以通过以下方式插入数据:

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

将数据作为分层JSON对象集插入，需要设置[input_format_import_nested_json=1](../operations/settings/settings.md#settings-input_format_import_nested_json)。

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

## Native {#native}

最高性能的格式。通过二进制格式的块进行写入和读取。对于每个块，该中的行数，列数，列名称和类型以及列的部分将被相继记录。换句话说，这种格式是`columnar`的 - 它不会将列转换为行。这种格式用于服务器间交互、命令行客户端和C++客户端与服务器交互。

您可以使用此格式快速生成只能由ClickHouse DBMS读取的格式。但自己处理这种格式是没有意义的。

## Null {#null}

没有输出。但是，查询已处理完毕，并且在使用命令行客户端时，数据将传输到客户端。这仅用于测试，包括性能测试。
显然，这种格式只适用于输出，不适用于解析。

## Pretty {#pretty}

将数据以表格形式输出，也可以使用ANSI转义字符在终端中设置颜色。
它会绘制一个完整的表格，每行数据在终端中占用两行。
每个结果块作为一个单独的表输出。这是必要的，以便在输出块时不缓冲结果(为了预先计算所有值的可见宽度，缓冲是必要的)。

[NULL](../sql-reference/syntax.md)输出为`ᴺᵁᴸᴸ`。

示例

``` sql
SELECT * FROM system.numbers limit 2 format Pretty;
```

``` text
┏━━━━━━━━┓
┃ number ┃
┡━━━━━━━━┩
│      0 │
├────────┤
│      1 │
└────────┘
```

Pretty的所有格式不进行字符转义。示例显示了[PrettyCompact](#prettycompact)格式:

``` sql
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

``` text
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

为避免将太多数据传输到终端，只打印前10,000行。 如果行数大于或等于10,000，则会显示消息`Showed first 10 000`。
该格式仅适用于输出查询结果，但不适用于解析输入(将数据插入到表中)。

Pretty格式支持输出合计值(当使用WITH TOTALS时)和极值(当`extremes`设置为1时)。在这些情况下，合计值和极值将输出在主要数据之后，在单独的表中。示例(显示为[PrettyCompact](#prettycompact)格式):

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

与[Pretty](#pretty)格式不一样的是`PrettyCompact`去掉了行之间的表格分割线，这样使得结果更加紧凑。
这种格式会在交互命令行客户端下默认使用。
``` sql
select * from system.numbers limit 2 format PrettyCompact;
```

``` text
┌─number─┐
│      0 │
│      1 │
└────────┘

```
## PrettyCompactMonoBlock {#prettycompactmonoblock}

与[PrettyCompact](#prettycompact)格式不一样的是，它支持10,000行数据缓冲，然后输出在一个表格中，而不分块。

## PrettyNoEscapes {#prettynoescapes}

与`Pretty`格式不一样的是，它不使用ANSI字符转义，这在浏览器显示数据以及在使用`watch`命令行工具是有必要的。

示例：

``` bash
watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

您可以使用HTTP接口来获取数据，显示在浏览器中。

### PrettyCompactNoEscapes {#prettycompactnoescapes}

用法类似上述。

### PrettySpaceNoEscapes {#prettyspacenoescapes}

用法类似上述。

## PrettySpace {#prettyspace}

与[PrettyCompact](#prettycompact)格式不一样的是，它使用空格来代替网格来显示数据。
``` sql
select * from system.numbers limit 2 format PrettySpace;
```
``` text
number

     0
     1
```

## RowBinary {#rowbinary}

以二进制格式逐行格式化和解析数据。行和值连续列出，没有分隔符。
这种格式比 Native 格式效率低，因为它是基于行的。

整数使用固定长度的小端表示法。 例如，UInt64 使用8个字节。
DateTime 被表示为 UInt32 类型的Unix 时间戳值。
Date 被表示为 UInt16 对象，它的值为自1970-01-01以来经过的天数。
字符串表示为 varint 长度(无符号 [LEB128](https://en.wikipedia.org/wiki/LEB128))，后跟字符串的字节数。
FixedString 被简单地表示为字节序列。

数组表示为 varint 长度(无符号 [LEB128](https://en.wikipedia.org/wiki/LEB128))，后跟有序的数组元素。

对于 [NULL](../sql-reference/syntax.md#null-literal) 的支持， 一个为 1 或 0 的字节会加在每个 [可为空](../sql-reference/data-types/nullable.md) 值前面。如果为 1, 那么该值就是 `NULL`。 如果为 0，则不为 `NULL`。

## RowBinaryWithNamesAndTypes {#rowbinarywithnamesandtypes}

类似于 [RowBinary](#rowbinary)，但添加了头部信息:

-   [LEB128](https://en.wikipedia.org/wiki/LEB128)-编码列数(N)
-   N `String`s指定列名
-   N `String`s指定列类型

## Values {#data-format-values}

在括号中打印每一行。行由逗号分隔。最后一行之后没有逗号。括号内的值也用逗号分隔。数字以十进制格式输出，不含引号。 数组以方括号输出。字符串、日期、日期时间用引号包围输出。转义字符的解析规则与 [TabSeparated](#tabseparated) 格式类似。 在格式化过程中，不插入额外的空格，但在解析过程中，空格是被允许并跳过的(除了数组值之外的空格，这是不允许的)。[NULL](../sql-reference/syntax.md) 为 `NULL`。

以 Values 格式传递数据时需要转义的最小字符集是：单引号和反斜线。

这是 `INSERT INTO t VALUES ...` 中可以使用的格式，但您也可以将其用于查询结果。

另见：[input_format_values_interpret_expressions](https://clickhouse.com/docs/en/operations/settings/settings/#settings-input_format_values_interpret_expressions)和[input_format_values_deduce_templates_of_expressions](https://clickhouse.com/docs/en/operations/settings/settings/#settings-input_format_values_deduce_templates_of_expressions)。

## Vertical {#vertical}

根据指定的列名，打印出每一行的值。这种格式适用于具有大量的列时，显示几个列。[NULL](../sql-reference/syntax.md) 输出为 `ᴺᵁᴸᴸ`。

示例:

``` sql
SELECT * FROM t_null FORMAT Vertical
```

    Row 1:
    ──────
    x: 1
    y: ᴺᵁᴸᴸ

该格式仅适用于输出查询结果，但不适用于解析输入(将数据插入到表中)。

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
就像JSON一样，将无效的 UTF-8 字符都替换成字符�，以便输出文本将包含有效的 UTF-8 字符序列。

在字符串值中，字符 `<` 和 `＆` 被转义为 `&lt;` 和 `&amp;`。

数组输出类似于 `<array> <elem> Hello </ elem> <elem> World </ elem> ... </ array>`，元组输出类似于 `<tuple> <elem> Hello </ elem> <elem> World </ ELEM> ... </tuple>` 。

## CapnProto {#capnproto}

Cap’n Proto 是一种二进制消息格式，类似Protobuf和Thriftis，但与 JSON 或 MessagePack 格式不一样。

Cap’n Proto 消息格式是严格类型的，而不是自我描述，这意味着它们需要架构描述。架构描述可以实时地应用，并针对每个查询进行缓存。

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits
       GROUP BY SearchPhrase FORMAT CapnProto SETTINGS schema = 'schema:Message'
```

其中 `schema.capnp` 描述如下：6y2

    struct Message {
      SearchPhrase @0 :Text;
      c @1 :Uint64;
    }

格式文件存储的目录可以在服务配置中的 [format_schema_path](../operations/server-configuration-parameters/settings.md) 指定。

Cap’n Proto 反序列化是很高效的，通常不会增加系统的负载。

## Protobuf {#protobuf}

Protobuf-是一个 [Protocol Buffers](https://developers.google.com/protocol-buffers/) 格式。

此格式需要外部格式描述文件(proto文件)。 该描述文件会进行缓存，以备后续查询。
ClickHouse支持 `proto2` 和 `proto3` 语法的proto文件，支持重复/可选/必填字段。

使用示例:

``` sql
SELECT * FROM test.table FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

``` bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT Protobuf SETTINGS format_schema='schemafile:MessageType'"
```

proto文件 `schemafile.proto` 看起来像这样:

``` capnp
syntax = "proto3";

message MessageType {
  string name = 1;
  string surname = 2;
  uint32 birthDate = 3;
  repeated string phoneNumbers = 4;
};
```
Clickhouse通过字段名称来对应列名称。字段名称不区分大小写，`_`与`.`视为相同符号。如果Proto文件指定的字段类型与列类型不相符，会进行转换。

支持Protobuf嵌套消息。 例如，对于下面Proto文件中的z字段：

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

ClickHouse会试图找到一个名为 `x.y.z` (或 `x_y_z` 或 `X.y_Z` 等)的列。
嵌套消息适用于输入或输出一个 [嵌套数据结构](../sql-reference/data-types/nested-data-structures/nested.md).

在protobuf模式中定义的默认值，如下：

``` capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

该默认值会被忽略，Clickhouse会使用 [表默认值](../sql-reference/statements/create.md#create-default-values)作为默认值。

ClickHouse在输入和输出protobuf消息采用`length-delimited` 格式。
这意味着每个消息之前，应该写它的长度作为一个 [varint](https://developers.google.com/protocol-buffers/docs/encoding#varints).
另请参阅 [如何在流行语言中读取/写入长度分隔的protobuf消息](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

## Avro {#data-format-avro}

[Apache Avro](http://avro.apache.org/) 是在Apache Hadoop项目中开发的面向行的数据序列化框架。

ClickHouse Avro格式支持读取和写入 [Avro数据文件](http://avro.apache.org/docs/current/spec.html#Object+Container+Files).

### 数据类型匹配{#sql_reference/data_types-matching} {#data-types-matching-sql_referencedata_types-matching}

下表显示了支持的数据类型以及它们如何匹配ClickHouse [数据类型](../sql-reference/data-types/index.md) 在 `INSERT` 和 `SELECT` 查询。

| Avro数据类型 `INSERT`                       | ClickHouse数据类型                                                                                                | Avro数据类型 `SELECT`        |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------|------------------------------|
| `boolean`, `int`, `long`, `float`, `double` | [Int(8/16/32)](../sql-reference/data-types/int-uint.md), [UInt(8/16/32)](../sql-reference/data-types/int-uint.md) | `int`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](../sql-reference/data-types/int-uint.md), [UInt64](../sql-reference/data-types/int-uint.md)               | `long`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](../sql-reference/data-types/float.md)                                                                   | `float`                      |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](../sql-reference/data-types/float.md)                                                                   | `double`                     |
| `bytes`, `string`, `fixed`, `enum`          | [String](../sql-reference/data-types/string.md)                                                                   | `bytes`                      |
| `bytes`, `string`, `fixed`                  | [FixedString(N)](../sql-reference/data-types/fixedstring.md)                                                       | `fixed(N)`                   |
| `enum`                                      | [Enum(8\|16)](../sql-reference/data-types/enum.md)                                                                 | `enum`                       |
| `array(T)`                                  | [Array(T)](../sql-reference/data-types/array.md)                                                                   | `array(T)`                   |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](../sql-reference/data-types/date.md)                                                                  | `union(null, T)`             |
| `null`                                      | [Nullable(Nothing)](../sql-reference/data-types/special-data-types/nothing.md)                                          | `null`                       |
| `int (date)` \*                             | [Date](../sql-reference/data-types/date.md)                                                                       | `int (date)` \*              |
| `long (timestamp-millis)` \*                | [DateTime64(3)](../sql-reference/data-types/datetime.md)                                                          | `long (timestamp-millis)` \* |
| `long (timestamp-micros)` \*                | [DateTime64(6)](../sql-reference/data-types/datetime.md)                                                          | `long (timestamp-micros)` \* |

\* [Avro逻辑类型](http://avro.apache.org/docs/current/spec.html#Logical+Types)

不支持的Avro数据类型: `record` (非根架构), `map`

不支持的Avro逻辑数据类型:  `time-millis`, `time-micros`, `duration`

### 插入数据 {#inserting-data}

将Avro文件中的数据插入ClickHouse表:

``` bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

输入Avro文件的根架构必须是 `record` 类型。

Clickhouse通过字段名称来对应架构的列名称。字段名称区分大小写。未使用的字段会被跳过。

ClickHouse表列的数据类型可能与插入的Avro数据的相应字段不同。 插入数据时，ClickHouse根据上表解释数据类型，然后通过 [Cast](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) 将数据转换为相应的列类型。

### 选择数据 {#selecting-data}

从ClickHouse表中选择数据到Avro文件:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

列名必须:

-   以 `[A-Za-z_]` 开始
-   随后只包含 `[A-Za-z0-9_]`

输出Avro文件压缩和同步间隔可以经由 [output_format_avro_codec](../operations/settings/settings.md#settings-output_format_avro_codec) 和 [output_format_avro_sync_interval](../operations/settings/settings.md#settings-output_format_avro_sync_interval) 设置。

## AvroConfluent {#data-format-avro-confluent}

AvroConfluent支持解码单个对象的Avro消息，这常用于 [Kafka](https://kafka.apache.org/) 和 [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html)。

每个Avro消息都嵌入了一个架构id，该架构id可以在架构注册表的帮助下解析为实际架构。

模式解析后会进行缓存。

架构注册表URL配置为 [format_avro_schema_registry_url](../operations/settings/settings.md#settings-format_avro_schema_registry_url)

### 数据类型匹配{#sql_reference/data_types-matching-1} {#data-types-matching-sql_referencedata_types-matching-1}

和 [Avro](#data-format-avro)相同。

### 用途 {#usage}

要快速验证架构解析，您可以使用 [kafkacat](https://github.com/edenhill/kafkacat) 与 [clickhouse-local](../operations/utilities/clickhouse-local.md):

``` bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```

使用 `AvroConfluent` 与 [Kafka](../engines/table-engines/integrations/kafka.md):

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
    设置 `format_avro_schema_registry_url` 需要写入配置文件`users.xml`以在Clickhouse重启后，该设置仍为您的设定值。您也可以在使用Kafka引擎的时候指定该设置。


## Parquet {#data-format-parquet}

[Apache Parquet](http://parquet.apache.org/) 是Hadoop生态系统中普遍使用的列式存储格式。 ClickHouse支持此格式的读写操作。

### 数据类型匹配{#sql_reference/data_types-matching-2} {#data-types-matching-sql_referencedata_types-matching-2}

下表显示了Clickhouse支持的数据类型以及它们在 `INSERT` 和 `SELECT` 查询如何对应Clickhouse的 [data types](../sql-reference/data-types/index.md) 。

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
| `DATE32`                   | [Date](../sql-reference/data-types/date.md)              | `UINT16`                   |
| `DATE64`, `TIMESTAMP`      | [DateTime](../sql-reference/data-types/datetime.md)      | `UINT32`                   |
| `STRING`, `BINARY`         | [String](../sql-reference/data-types/string.md)          | `STRING`                   |
| —                          | [FixedString](../sql-reference/data-types/fixedstring.md) | `STRING`                   |
| `DECIMAL`                  | [Decimal](../sql-reference/data-types/decimal.md)         | `DECIMAL`                  |

ClickHouse支持对 `Decimal` 类型设置精度。 `INSERT` 查询将 Parquet `DECIMAL` 类型视为ClickHouse `Decimal128` 类型。

不支持的Parquet数据类型: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

ClickHouse表列的数据类型可能与插入的Parquet数据的相应字段不同。 插入数据时，ClickHouse根据上表解释数据类型，然后 [Cast](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) 为ClickHouse表列设置的数据类型的数据。

### 插入和选择数据 {#inserting-and-selecting-data}

您可以通过以下命令将Parquet数据从文件插入到ClickHouse表中:

``` bash
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT Parquet"
```

您可以从ClickHouse表中选择数据，并通过以下命令将它们保存到Parquet格式的文件中:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Parquet" > {some_file.pq}
```

要与Hadoop交换数据，您可以使用 [HDFS table engine](../engines/table-engines/integrations/hdfs.md).

## Arrow {#data-format-arrow}
[Apache Arrow](https://arrow.apache.org/)是一种用于内存数据库的格式，共有两种模式，文件与流模式。Clickhouse支持对这两种格式进行读写。

`Arrow`对应的是文件模式，这种格式适用于内存的随机访问。

## ArrowStream {#data-format-arrow}
`ArrowStream`对应的是Arrow的流模式，这种格式适用于内存的流式处理。

## ORC {#data-format-orc}
[Apache ORC](https://orc.apache.org/) 是Hadoop生态系统中普遍存在的列式存储格式。

### 数据类型匹配{#sql_reference/data_types-matching-3} {#data-types-matching-sql_referencedata_types-matching-3}

下表显示了支持的数据类型以及它们如何在`SELECT`与`INSERT`查询中匹配ClickHouse的 [数据类型](../sql-reference/data-types/index.md)。

| ORC 数据类型 (`INSERT`) | ClickHouse 数据类型                                | ORC 数据类型 (`SELECT`) |
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
| `-`                      | [Array](../sql-reference/data-types/array.md)       | `LIST`                   |

ClickHouse支持的可配置精度的 `Decimal` 类型。 `INSERT` 查询将ORC格式的 `DECIMAL` 类型视为ClickHouse的 `Decimal128` 类型。

不支持的ORC数据类型: `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

ClickHouse表列的数据类型不必匹配相应的ORC数据字段。 插入数据时，ClickHouse根据上表解释数据类型，然后 [Cast](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) 将数据转换为ClickHouse表列的数据类型集。

### 插入数据 {#inserting-data-1}

您可以通过以下命令将文件中的ORC数据插入到ClickHouse表中:

``` bash
$ cat filename.orc | clickhouse-client --query="INSERT INTO some_table FORMAT ORC"
```

### 选择数据 {#selecting-data-1}

您可以通过以下命令将ClickHouse表中某些数据导出到ORC文件:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT ORC" > {filename.orc}
```

要与Hadoop交换数据，您可以使用 [HDFS表引擎](../engines/table-engines/integrations/hdfs.md).

## LineAsString {#lineasstring}
这种格式下，每行输入数据都会当做一个字符串。这种格式仅适用于仅有一列[String](https://clickhouse.com/docs/en/sql-reference/data-types/string/)类型的列的表。其余列必须设置为[DEFAULT](https://clickhouse.com/docs/en/sql-reference/statements/create/table/#default)、[MATERIALIZED](https://clickhouse.com/docs/en/sql-reference/statements/create/table/#materialized)或者被忽略。

### 示例：
查询如下：
``` sql
DROP TABLE IF EXISTS line_as_string;
CREATE TABLE line_as_string (field String) ENGINE = Memory;
INSERT INTO line_as_string FORMAT LineAsString "I love apple", "I love banana", "I love orange";
SELECT * FROM line_as_string;
```
结果如下：
``` text
┌─field─────────────────────────────────────────────┐
│ "I love apple", "I love banana", "I love orange"; │
└───────────────────────────────────────────────────┘
```
## Regexp {#regexp}
每一列输入数据根据正则表达式解析。使用`Regexp`格式时，可以使用如下设置：

-  `format_regexp`，[String](https://clickhouse.com/docs/en/sql-reference/data-types/string/)类型。包含[re2](https://github.com/google/re2/wiki/Syntax)格式的正则表达式。
- `format_regexp_escaping_rule`，[String](https://clickhouse.com/docs/en/sql-reference/data-types/string/)类型。支持如下转义规则：
  - CSV(规则相同于[CSV](https://clickhouse.com/docs/zh/interfaces/formats/#csv))
  - JSON(相同于[JSONEachRow](https://clickhouse.com/docs/zh/interfaces/formats/#jsoneachrow))
  - Escaped(相同于[TSV](https://clickhouse.com/docs/zh/interfaces/formats/#tabseparated))
  - Quoted(相同于[Values](https://clickhouse.com/docs/zh/interfaces/formats/#data-format-values))
  - Raw(将整个子匹配项进行提取，不转义)
- `format_regexp_skip_unmatched`，[UInt8](https://clickhouse.com/docs/zh/sql-reference/data-types/int-uint/)类型。当`format_regexp`表达式没有匹配到结果时是否抛出异常。可为0或1。

### 用法 {#usage-1}
`format_regexp`设置会应用于每一行输入数据。正则表达式的子匹配项数必须等于输入数据期望得到的列数。
每一行输入数据通过换行符`\n`或者`\r\n`分隔。
匹配到的子匹配项会根据每一列的数据格式进行解析，转义规则根据`format_regexp_escaping_rule`进行。
当正则表达式对某行没有匹配到结果，`format_regexp_skip_unmatched`设为1时，该行会被跳过。`format_regexp_skip_unmatched`设为0时，会抛出异常。

### 示例 {#example-1}
设有如下data.tsv:
``` text
id: 1 array: [1,2,3] string: str1 date: 2020-01-01
id: 2 array: [1,2,3] string: str2 date: 2020-01-02
id: 3 array: [1,2,3] string: str3 date: 2020-01-03
```
与表:
``` sql
CREATE TABLE imp_regex_table (id UInt32, array Array(UInt32), string String, date Date) ENGINE = Memory;
```
导入命令：
``` bash
$ cat data.tsv | clickhouse-client  --query "INSERT INTO imp_regex_table FORMAT Regexp SETTINGS format_regexp='id: (.+?) array: (.+?) string: (.+?) date: (.+?)', format_regexp_escaping_rule='Escaped', format_regexp_skip_unmatched=0;"
```
查询：
``` sql
SELECT * FROM imp_regex_table;
```
结果：
``` text
┌─id─┬─array───┬─string─┬───────date─┐
│  1 │ [1,2,3] │ str1   │ 2020-01-01 │
│  2 │ [1,2,3] │ str2   │ 2020-01-02 │
│  3 │ [1,2,3] │ str3   │ 2020-01-03 │
└────┴─────────┴────────┴────────────┘
```



## RawBLOB {#rawblob}
这种格式下，所有输入数据视为一个值。该格式仅适用于仅有一String类型的列的表。输出时，使用二进制格式输出。当输出结果不唯一时，输出是有歧义的，并且不能通过该输出还原原数据。
下面是`RawBLOB`与[TabSeparatedRaw](https://clickhouse.com/docs/zh/interfaces/formats/#tabseparatedraw)的对比：

`RawBloB`:

- 二进制格式输出，无转义。
- 值之间没有分隔符。
- 每行最后的值后面没有换行符。

`TabSeparatedRaw`:

- 数据无转义输出。
- 每行的值通过制表符分隔。
- 每行最后的值得后面有换行符。

下面是`RawBLOB`与[RowBinary](https://clickhouse.com/docs/zh/interfaces/formats/#rowbinary)的对比：

`RawBloB`:

- 字符串前面没有表示长度的标志

`RowBinary`:

- 字符串前面有变长标志([LEB128](https://en.wikipedia.org/wiki/LEB128)格式表示)，用于表示字符串长度，后接字符串内容。

当传入空数据，Clickhouse会抛出异常：
``` text
Code: 108. DB::Exception: No data to insert
```
### 示例 {#example-4}
``` bash
$ clickhouse-client --query "CREATE TABLE {some_table} (a String) ENGINE = Memory;"
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT RawBLOB"
$ clickhouse-client --query "SELECT * FROM {some_table} FORMAT RawBLOB" | md5sum
```
结果：
``` text
f9725a22f9191e064120d718e26862a9  -
```

## 格式架构 {#formatschema}

包含格式架构的文件名由设置 `format_schema`指定.当使用`CapnProto` 或 `Protobuf`其中一种格式时，需要设置该项.
格式架构为架构文件名和此文件中消息类型的组合，用冒号分隔,例如 `schemafile.proto:MessageType`.
如果文件具有格式的标准扩展名(例如, `Protobuf`格式的架构文件标准扩展名为`.proto`),它可以被省略，在这种情况下，格式模式如下所示 `schemafile:MessageType`.

如果您通过[Client](../interfaces/cli.md) 在 [交互模式](https://clickhouse.com/docs/zh/interfaces/cli/#cli_usage)下输入或输出数据，格式架构中指定的文件名可以使用绝对路径或客户端当前目录的相对路径。
如果在[批处理模式](https://clickhouse.com/docs/zh/interfaces/cli/#cli_usage)下使用客户端，则由于安全原因，架构的路径必须使用相对路径。

如果您通过 [HTTP接口](../interfaces/http.md)输入或输出数据，格式架构中指定的文件名应该位于服务器设置的[format_schema_path](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-format_schema_path)指定的目录中。


## 跳过错误 {#skippingerrors}

一些格式，如 `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` 和 `Protobuf` 如果发生解析错误，可以跳过引发错误的行，并从下一行开始继续解析。 详情请见设置[input_format_allow_errors_num](../operations/settings/settings.md#settings-input_format_allow_errors_num) 和
[input_format_allow_errors_ratio](../operations/settings/settings.md#settings-input_format_allow_errors_ratio) 。

限制:
- 在解析错误的情况下 `JSONEachRow` 跳过该行的所有数据，直到遇到新行(或EOF)，所以行必须由换行符分隔以正确统计错误行的数量。
- `Template` 和 `CustomSeparated` 在最后一列之后和行之间使用分隔符来查找下一行的开头，所以跳过错误只有在行分隔符和列分隔符其中至少有一个不为空时才有效。



[来源文章](https://clickhouse.com/docs/zh/interfaces/formats/) <!--hide-->
