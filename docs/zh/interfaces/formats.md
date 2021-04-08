---
toc_priority: 21
toc_title: 输入/输出格式
---

# 输入/输出格式 {#formats}

ClickHouse可以接受和返回各种格式的数据。输入支持的格式可以用来解析提供给`INSERT`的数据，可以从文件备份表(如File, URL或HDFS)执行`SELECT`，或者读取外部字典。输出支持的格式可用于获取`SELECT`的结果，并支持执行`INSERT`文件的表中。

以下是支持的格式:

| 格式                                                                                    | 输入   | 输出   |
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
| [VerticalRaw](#verticalraw)                                                             | ✗     | ✔      |
| [JSON](#json)                                                                           | ✗     | ✔      |
| [JSONAsString](#jsonasstring)                                                           | ✔     | ✗      |
| [JSONString](#jsonstring)                                                               | ✗     | ✔      |
| [JSONCompact](#jsoncompact)                                                             | ✗     | ✔      |
| [JSONCompactString](#jsoncompactstring)                                                 | ✗     | ✔      |
| [JSONEachRow](#jsoneachrow)                                                             | ✔     | ✔      |
| [JSONEachRowWithProgress](#jsoneachrowwithprogress)                                     | ✗     | ✔      |
| [JSONStringEachRow](#jsonstringeachrow)                                                 | ✔     | ✔      |
| [JSONStringEachRowWithProgress](#jsonstringeachrowwithprogress)                         | ✗     | ✔      |
| [JSONCompactEachRow](#jsoncompacteachrow)                                               | ✔     | ✔      |
| [JSONCompactEachRowWithNamesAndTypes](#jsoncompacteachrowwithnamesandtypes)             | ✔     | ✔      |
| [JSONCompactStringEachRow](#jsoncompactstringeachrow)                                   | ✔     | ✔      |
| [JSONCompactStringEachRowWithNamesAndTypes](#jsoncompactstringeachrowwithnamesandtypes) | ✔     | ✔      |
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
| [ORC](#data-format-orc)                                                                 | ✔     | ✗      |
| [RowBinary](#rowbinary)                                                                 | ✔     | ✔      |
| [RowBinaryWithNamesAndTypes](#rowbinarywithnamesandtypes)                               | ✔     | ✔      |
| [Native](#native)                                                                       | ✔     | ✔      |
| [Null](#null)                                                                           | ✗     | ✔      |
| [XML](#xml)                                                                             | ✗     | ✔      |
| [CapnProto](#capnproto)                                                                 | ✔     | ✗      |
| [LineAsString](#lineasstring)                                                           | ✔     | ✗      |

您可以使用ClickHouse设置控制一些格式处理参数。更多详情设置请参考[设置](../operations/settings/settings.md) 

## TabSeparated {#tabseparated}

在TabSeparated分隔格式中，数据按行写入。每行包含由制表符分隔的值。每个值后跟一个制表符，除了行中最后一个值后跟换行。在任何地方都采用严格的Unix换行。最后一行还必须在末尾包含换行。值以文本格式编写，不包含引号，并使用转义的特殊字符。

这种格式也可以用`TSV`来表示。

`TabSeparated`格式便于使用自定义程序和脚本处理数据。默认情况下，它在HTTP接口和命令行客户端的批处理模式中使用。这种格式还允许在不同dbms之间传输数据。例如，您可以从MySQL获取转储并将其上传到ClickHouse，反之亦然。

`TabSeparated`格式支持输出total值(与TOTALS一起使用时)和extreme值(当`extreme`被设置为1时)。在这种情况下，total值和extreme值会在主数据后输出。主要结果、总值和极值之间用空行分隔。示例:

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated``
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

### 数据格式化 {#data-formatting}

整数是用十进制形式写的。数字可以在开头包含一个额外的`+`字符(解析时忽略，格式化时不记录)。非负数不能包含负号。在读取时，允许将空字符串解析为零，或者(对于有符号类型)将仅由一个负号组成的字符串解析为零。不符合相应数据类型的数字可以被解析为不同的数字，而不会出现错误消息。

浮点数以十进制形式书写。`.`号用作十进制分隔符。支持指数符号，如`inf`、`+inf`、`-inf`和`nan`。浮点数的条目可以以小数点开始或结束。
在格式化期间，浮点数可能会丢失准确性。
在解析期间，并不严格要求读取与机器可以表示的最接近的数值。

日期以YYYY-MM-DD格式编写，并以相同的格式解析，但使用任何字符作为分隔符。
日期和时间以`YYYY-MM-DD hh:mm:ss`的格式书写，并以相同的格式解析，但使用任何字符作为分隔符。
这一切都发生在客户端或服务器启动时的系统时区(取决于它们对数据的格式)。对于带有时间的日期，夏时制时间未指定。因此，如果转储在夏令时有时间，则转储不会明确地与数据匹配，解析将选择这两次中的一次。
在读取操作期间，不正确的日期和具有时间的日期可以使用自然溢出或null日期和时间进行分析，而不会出现错误消息。

有个例外情况，Unix时间戳格式也支持用时间解析日期(如果它恰好由10个十进制数字组成)。其结果与时间区域无关。格式`YYYY-MM-DD hh:mm:ss`和`NNNNNNNNNN`是自动区分的。

字符串以反斜杠转义的特殊字符输出。下面的转义序列用于输出:`\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`。解析还支持`\a`、`\v`和`\xHH`(十六进制转义字符)和任何`\c`字符，其中`c`是任何字符(这些序列被转换为`c`)。因此，读取数据支持这样一种格式，即可以将换行符写成`\n`或`\`，或者写成换行符。例如，字符串`Hello world`在单词之间有换行符，而不是空格，可以用以下语法进行解析:

``` text
Hello\nworld

Hello\
world
```

第二种形式是支持的，因为MySQL读取tab-separated格式数据集的时候也会使用它。

在TabSeparated分隔格式传递数据时需要转义的最小字符集:`Tab`、换行符(LF)和反斜杠。

只有一小部分符号被转义。您可以很容易地找到一个字符串值，而您的终端将在输出中不显示它。

数组写在方括号内的逗号分隔值列表中。数组中的数字项按正常格式进行格式化。`Date`和`DateTime`类型用单引号表示。字符串使用与上面相同的转义规则在单引号中编写。

[NULL](../sql-reference/syntax.md)将输出为`\N`。

[Nested](../sql-reference/data-types/nested-data-structures/nested.md)结构的每个元素都表示为数组。

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

与`TabSeparated`格式的不同之处在于，写入的行没有转义。
使用这种格式解析时，每个字段中不允许使用制表符或换行符。

这种格式也可以使用名称`TSVRaw`来表示。

## TabSeparatedWithNames {#tabseparatedwithnames}

与`TabSeparated`格式不同的是列名写在第一行。
在解析过程中，第一行被完全忽略。不能使用列名来确定它们的位置或检查它们的正确性。
(将来可能会添加对头行解析的支持。)

这种格式也可以使用名称`TSVWithNames`来表示。

## TabSeparatedWithNamesAndTypes {#tabseparatedwithnamesandtypes}

与`TabSeparated`格式不同的是列名写在第一行，而列类型写在第二行。
在解析过程中，将完全忽略第一行和第二行。

这种格式也可以使用名称`TSVWithNamesAndTypes`来表示。

## Template {#format-template}

此格式允许指定带有占位符的自定义格式字符串，这些占位符用于指定转义规则。

它使用设置`format_schema`, `format_schema_rows`, `format_schema_rows_between_delimiter`以及其他格式的一些设置(例如转义`JSON`时使用`output_format_json_quote_64bit_integers`)

设置`format_template_row`指定文件的路径，该文件包含以下语法的行格式字符串:

`delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N`,

其中，`delimiter_i`是值之间的分隔符(`$`符号可以转义为`$$`)，
`column_i`是要选择或插入其值的列的名称或索引(如果为空，则跳过该列)，
`serializeAs_i`是列值的转义规则。支持以下转义规则:

-   `CSV`, `JSON`, `XML` (类似于相同名称的格式)
-   `Escaped` (类似于`TSV`)
-   `Quoted` (类似于`Values`)
-   `Raw` (类似于`TSVRaw`)
-   `None`

如果省略了转义规则，那么将使用`None`。`XML`和`Raw`只适用于输出。

对于下面的格式字符串:

      `Search phrase: ${SearchPhrase:Quoted}, count: ${c:Escaped}, ad price: $$${price:JSON};`

`SearchPhrase`、`c`和`price`列的值被转义为`quotation`、`Escaped`和`JSON`将分别在`Search phrase:`， `， count: `， `， ad price: $`和`;`分隔符之间打印(用于选择)或expected(用于插入)。例如:

`Search phrase: 'bathroom interior design', count: 2166, ad price: $3;`

`format_template_rows_between_delimiter`设置指定行之间的分隔符，它将打印(或expected)在每一行之后，最后一行除外(默认为`\n`)。

设置`format_template_resultset`指定文件路径，该文件包含resultset的格式字符串。resultset的格式字符串与row的格式字符串具有相同的语法，允许指定前缀、后缀和打印一些附加信息的方法。它包含以下占位符而不是列名:

-   `data` `format_template_row`格式的数据行，由`format_template_rows_between_delimiter`分隔。此占位符必须是格式字符串中的第一个占位符。
-   `totals` `format_template_row`格式的总值(和WITH TOTALS一起使用)
-   `min` `format_template_row`格式的最小值(当极值设置为1时)
-   `max` `format_template_row`格式的最大值(当极值设置为1时)
-   `rows` 输出行的总数
-   `rows_before_limit` 没有LIMIT的最小行数。仅当查询包含LIMIT时输出。如果查询包含GROUP BY，那么rows_before_limit_at_least就是没有LIMIT的确切行数。
-   `time` 请求执行时间（秒）
-   `rows_read` 已读取的行数
-   `bytes_read` 已读取（未压缩）的字节数

占位符`data`、`totals`、`min`和`max`必须没有指定转义规则(或者必须显式指定`None`)。其余占位符可以指定任何转义规则。
如果`format_template_resultset`设置为空字符串，则使用`${data}`作为默认值。
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

`PageViews`, `UserID`, `Duration`和`Sign` 内部占位符是表中列的名称。将忽略行中`Useless field`后面和后缀中`\nTotal rows:`之后的值。
输入数据中的所有分隔符必须严格等于指定格式字符串中的分隔符。

## TemplateIgnoreSpaces {#templateignorespaces}

这种格式只适用于输入。
类似于`Template`，但跳过输入流中分隔符和值之间的空白字符。但是，如果格式字符串包含空格字符，这些字符将会出现在输入流中。还允许指定空占位符(`${}`或`${:None}`)来将一些分隔符分割为单独的部分，以忽略它们之间的空格。这种占位符仅用于跳过空白字符。
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

类似于TabSeparated，但是输出的值是name=value格式。名称的转义方式与TabSeparated格式相同，=符号也是转义的。

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

[NULL](../sql-reference/syntax.md)格式为`\N`。

``` sql
SELECT * FROM t_null FORMAT TSKV
```

``` text
x=1    y=\N
```

当有大量的小列时，这种格式是无效的，并且通常没有理由使用它。不过，就效率而言，它并不比JSONEachRow差。
这种格式支持数据输出和解析。对于解析，不同列的值支持任何顺序。省略某些值是可以接受的——它们被视为与其默认值相等。在这种情况下，0和空白行被用作默认值。不支持在表中指定的复杂值作为缺省值。

解析允许存在不带等号或值的附加字段`tskv`。此字段被忽略。

## CSV {#csv}

按`,`分隔的数据格式([RFC](https://tools.ietf.org/html/rfc4180))。

格式化时，行是用双引号括起来的。字符串中的双引号会以两个双引号输出，除此之外没有其他规则来做字符转义了。日期和时间也会以双引号包括。数字的输出不带引号。值由一个单独的字符隔开，这个字符默认是`,`。行使用Unix换行符（LF）分隔。数组序列化成CSV规则如下：首先将数组序列化为`TabSeparated`格式的字符串，然后将结果字符串用双引号包括输出到`CSV`。`CSV`格式的元组被序列化为单独的列（即它们在元组中的嵌套关系会丢失）。

``` bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

\* 默认情况下间隔符是`,` ，在[format_csv_delimiter](../operations/settings/settings.md#settings-format_csv_delimiter)中可以了解更多分隔符配置。

解析的时候，可以使用或不使用引号来解析所有值。支持双引号和单引号。行也可以不用引号排列。在这种情况下，它们被解析为逗号或换行符（`CR或`LF`）。在解析不带引号的行时，若违反`RFC`规则，会忽略前缀和结尾的空格和制表符。对于换行，全部支持Unix（LF），Windows（CR LF）和Mac OS Classic（CR LF）。

如果启用[input_format_defaults_for_omitted_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields)，空的末尾加引号的输入值将替换为相应列的默认值。

`NULL`被格式化为`\N`或`NULL`或一个空的非引号字符串(详见配置[input_format_csv_unquoted_null_literal_as_null](../operations/settings/settings.md#settings-input_format_csv_unquoted_null_literal_as_null)或[input_format_defaults_for_omitted_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields))。

`CSV`格式支持输出总数和极值的方式与`TabSeparated`相同。

## CSVWithNames {#csvwithnames}

会输出带头部的信息(字段列表)，和`TabSeparatedWithNames`一样。

## CustomSeparated {#format-customseparated}

类似于[Template](#format-template)， 但它打印或读取所有列和使用转义规则在设置`format_custom_escaping_rule`和分隔符设置`format_custom_field_delimiter`,`format_custom_row_before_delimiter`,`format_custom_row_after_delimiter`,`format_custom_row_between_delimiter`,`format_custom_result_before_delimiter`,`format_custom_result_after_delimiter`中,而不是从格式字符串。
也有`CustomSeparatedIgnoreSpaces`格式，这是类似于`TemplateIgnoreSpaces`。

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

JSON与JavaScript兼容。为了确保这一点，一些字符被另外转义：斜线`/`被转义为`\/`; 替代的换行符`U+2028`和`U+2029`会打断一些浏览器解析，它们会被转义为`\uXXXX`。 ASCII控制字符被转义：退格，换页，换行，回车和水平制表符被替换为`\b`，`\f`，`\n`，`\r`，`\t` 作为使用`\uXXXX`序列的00-1F范围内的剩余字节。 无效的UTF-8序列更改为替换字符，因此输出文本将包含有效的UTF-8序列。 为了与JavaScript兼容，默认情况下，Int64和UInt64整数用双引号引起来。要除去引号，可以将配置参数`output_format_json_quote_64bit_integers`设置为0。

`rows` – 结果输出的行数。

`rows_before_limit_at_least`去掉 LIMIT过滤后的最小行总数。 只会在查询包含LIMIT条件时输出。
若查询包含 GROUP BY，`rows_before_limit_at_least`就是去掉LIMIT后过滤后的准确行数。

`totals` – 总值 （当使用TOTALS条件时）。

`extremes` – 极值（当extremes设置为1时）。

该格式仅适用于输出查询结果，但不适用于解析输入（将数据插入到表中）。

ClickHouse支持[NULL](../sql-reference/syntax.md), 在JSON输出中显示为`null`。若要在输出中启用`+nan`、`-nan`、`+inf`、`-inf`值，请设置[output_format_json_quote_denormals](../operations/settings/settings.md#settings-output_format_json_quote_denormals)为1。

**参考**

-   [JSONEachRow](#jsoneachrow)格式
-   [output_format_json_array_of_rows](../operations/settings/settings.md#output-format-json-array-of-rows)设置

## JSONString {#jsonstring}

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

## JSONAsString {#jsonasstring}

在这种格式中，一个JSON对象被解释为一个值。如果输入有几个JSON对象(逗号分隔)，它们将被解释为独立的行。

这种格式只能对具有单个字段类型的表进行解析[String](../sql-reference/data-types/string.md)。其余的列必须设置为[DEFAULT](../sql-reference/statements/create.md)或[MATERIALIZED](../sql-reference/statements/create.md)，或者忽略。一旦将整个JSON对象收集为字符串，就可以使用[JSON函数](../sql-reference/functions/json-functions.md)运行它。

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
## JSONCompactString {#jsoncompactstring}

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
// JSONCompactString
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
## JSONStringEachRow {#jsonstringeachrow}
## JSONCompactEachRow {#jsoncompacteachrow}
## JSONCompactStringEachRow {#jsoncompactstringeachrow}

使用这些格式时，ClickHouse会将行输出为分隔的、换行分隔的JSON值，但数据作为一个整体不是有效的JSON。

``` json
{"some_int":42,"some_str":"hello","some_tuple":[1,"a"]} // JSONEachRow
[42,"hello",[1,"a"]] // JSONCompactEachRow
["42","hello","(2,'a')"] // JSONCompactStringsEachRow
```

在插入数据时，应该为每一行提供一个单独的JSON值。

## JSONEachRowWithProgress {#jsoneachrowwithprogress}
## JSONStringEachRowWithProgress {#jsonstringeachrowwithprogress}

与`JSONEachRow`/`JSONStringEachRow`不同的是，ClickHouse还将生成作为JSON值的进度信息。

```json
{"row":{"'hello'":"hello","multiply(42, number)":"0","range(5)":[0,1,2,3,4]}}
{"row":{"'hello'":"hello","multiply(42, number)":"42","range(5)":[0,1,2,3,4]}}
{"row":{"'hello'":"hello","multiply(42, number)":"84","range(5)":[0,1,2,3,4]}}
{"progress":{"read_rows":"3","read_bytes":"24","written_rows":"0","written_bytes":"0","total_rows_to_read":"3"}}
```

## JSONCompactEachRowWithNamesAndTypes {#jsoncompacteachrowwithnamesandtypes}
## JSONCompactStringEachRowWithNamesAndTypes {#jsoncompactstringeachrowwithnamesandtypes}

与`JSONCompactEachRow`/`JSONCompactStringEachRow`不同的是，其中列名和类型被写入前两行。

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

- 对象中key-value的任何顺序。
- 省略一些值。

ClickHouse忽略元素之间的空格和对象后面的逗号。您可以在一行中传递所有对象。你不需要用换行符把它们分开。

**省略值处理**

ClickHouse将省略的值替换为对应的[data types](../sql-reference/data-types/index.md)默认值。

如果指定了`DEFAULT expr`，则ClickHouse根据属性使用不同的替换规则，详看[input_format_defaults_for_omitted_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields)设置。

参考下表：

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
当使用`insert_sample_with_metadata = 1`插入数据时，与使用`insert_sample_with_metadata = 0`插入数据相比，ClickHouse消耗更多的计算资源。

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

正如您在`Nested`数据类型描述中看到的，ClickHouse将嵌套结构的每个组件作为一个单独的列(`n.s`和`n.i`是我们的表)。您可以通过以下方式插入数据:

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

将数据作为分层JSON对象集插入[input_format_import_nested_json=1](../operations/settings/settings.md#settings-input_format_import_nested_json)。

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

最高性能的格式。通过二进制格式的块进行写入和读取。对于每个块，该中的行数，列数，列名称和类型以及列的部分将被相继记录。 换句话说，这种格式是`columnar`的 - 它不会将列转换为行。这是用于在服务器之间进行交互的本地界面中使用的格式，用于使用命令行客户端和C++客户端。

您可以使用此格式快速生成只能由ClickHouse DBMS读取的格式。但自己处理这种格式是没有意义的。

## Null {#null}

没有输出。但是，查询已处理完毕，并且在使用命令行客户端时，数据将传输到客户端。这仅用于测试，包括性能测试。
显然，这种格式只适用于输出，不适用于解析。

## Pretty {#pretty}

将数据以表格形式输出，也可以使用ANSI转义字符在终端中设置颜色。
它会绘制一个完整的表格，每行数据在终端中占用两行。
每个结果块作为一个单独的表输出。这是必要的，以便在输出块时不需要缓冲结果(为了预先计算所有值的可见宽度，缓冲是必要的)。

[NULL](../sql-reference/syntax.md)输出为`ᴺᵁᴸᴸ`。

示例(显示[PrettyCompact](#prettycompact)格式)

``` sql
SELECT * FROM t_null
```

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

行没有转义为Pretty\* 格式。示例显示了[PrettyCompact](#prettycompact)格式:

``` sql
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

``` text
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

为避免将太多数据传输到终端，只打印前10,000行。 如果行数大于或等于10,000，则会显示消息`Showed first 10 000`。
该格式仅适用于输出查询结果，但不适用于解析输入（将数据插入到表中）。

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

## PrettyCompactMonoBlock {#prettycompactmonoblock}

与[PrettyCompact](#prettycompact)格式不一样的是，它支持10,000行数据缓冲，然后输出在一个表格中，不会按照块来区分。

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

### PrettyCompactNoEscapes {#prettycompactnoescapes}

与前面的设置相同。

### PrettySpaceNoEscapes {#prettyspacenoescapes}

与前面的设置相同。

## PrettySpace {#prettyspace}

与[PrettyCompact](#prettycompact)格式不一样的是，它使用空格来代替网格来显示数据。

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

格式文件存储的目录可以在服务配置中的 [format_schema_path](../operations/server-configuration-parameters/settings.md) 指定。

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

### 数据类型匹配{#sql_reference/data_types-matching} {#data-types-matching-sql_referencedata_types-matching}

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

输出Avro文件压缩和同步间隔可以配置 [output_format_avro_codec](../operations/settings/settings.md#settings-output_format_avro_codec) 和 [output_format_avro_sync_interval](../operations/settings/settings.md#settings-output_format_avro_sync_interval) 分别。

## AvroConfluent {#data-format-avro-confluent}

AvroConfluent支持解码单对象Avro消息常用于 [卡夫卡](https://kafka.apache.org/) 和 [汇合的模式注册表](https://docs.confluent.io/current/schema-registry/index.html).

每个Avro消息都嵌入了一个架构id，该架构id可以在架构注册表的帮助下解析为实际架构。

模式解析后会进行缓存。

架构注册表URL配置为 [format_avro_schema_registry_url](../operations/settings/settings.md#settings-format_avro_schema_registry_url)

### 数据类型匹配{#sql_reference/data_types-matching-1} {#data-types-matching-sql_referencedata_types-matching-1}

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

### 数据类型匹配{#sql_reference/data_types-matching-2} {#data-types-matching-sql_referencedata_types-matching-2}

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

### 数据类型匹配{#sql_reference/data_types-matching-3} {#data-types-matching-sql_referencedata_types-matching-3}

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
应该位于指定的目录中 [format_schema_path](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-format_schema_path)
在服务器配置中。

[原始文章](https://clickhouse.tech/docs/en/interfaces/formats/) <!--hide-->

## 跳过错误 {#skippingerrors}

一些格式，如 `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` 和 `Protobuf` 如果发生解析错误，可以跳过断开的行，并从下一行开始继续解析。 看 [input_format_allow_errors_num](../operations/settings/settings.md#settings-input_format_allow_errors_num) 和
[input_format_allow_errors_ratio](../operations/settings/settings.md#settings-input_format_allow_errors_ratio) 设置。
限制:
-在解析错误的情况下 `JSONEachRow` 跳过所有数据，直到新行（或EOF），所以行必须由 `\n` 正确计算错误。
- `Template` 和 `CustomSeparated` 在最后一列之后使用分隔符，并在行之间使用分隔符来查找下一行的开头，所以跳过错误只有在其中至少有一个不为空时才有效。

[来源文章](https://clickhouse.tech/docs/zh/interfaces/formats/) <!--hide-->
