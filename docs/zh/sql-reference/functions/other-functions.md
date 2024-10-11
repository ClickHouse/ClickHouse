---
slug: /zh/sql-reference/functions/other-functions
---
# 其他函数 {#qi-ta-han-shu}

## 主机名() {#hostname}

返回一个字符串，其中包含执行此函数的主机的名称。 对于分布式处理，如果在远程服务器上执行此函数，则将返回远程服务器主机的名称。

## basename {#basename}

在最后一个斜杠或反斜杠后的字符串文本。 此函数通常用于从路径中提取文件名。

    basename( expr )

**参数**

-   `expr` — 任何一个返回[字符串](../../sql-reference/functions/other-functions.md)结果的表达式。[字符串](../../sql-reference/functions/other-functions.md)

**返回值**

一个String类型的值，其包含：

-   在最后一个斜杠或反斜杠后的字符串文本内容。

        如果输入的字符串以斜杆或反斜杆结尾，例如：`/`或`c:\`，函数将返回一个空字符串。

-   如果输入的字符串中不包含斜杆或反斜杠，函数返回输入字符串本身。

**示例**

``` sql
SELECT 'some/long/path/to/file' AS a, basename(a)
```

```response
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some\\long\\path\\to\\file' AS a, basename(a)
```

```response
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some-file-name' AS a, basename(a)
```

```response
┌─a──────────────┬─basename('some-file-name')─┐
│ some-file-name │ some-file-name             │
└────────────────┴────────────────────────────┘
```

## visibleWidth(x) {#visiblewidthx}

以文本格式（以制表符分隔）向控制台输出值时，计算近似宽度。
系统使用此函数实现Pretty格式。
以文本格式（制表符分隔）将值输出到控制台时，计算近似宽度。
这个函数被系统用于实现漂亮的格式。

`NULL` 表示为对应于 `NULL` 在 `Pretty` 格式。

    SELECT visibleWidth(NULL)

    ┌─visibleWidth(NULL)─┐
    │                  4 │
    └────────────────────┘

## toTypeName(x) {#totypenamex}

返回包含参数的类型名称的字符串。

如果将`NULL`作为参数传递给函数，那么它返回`Nullable（Nothing）`类型，它对应于ClickHouse中的内部`NULL`。

## 块大小() {#function-blocksize}

获取Block的大小。
在ClickHouse中，查询始终工作在Block（包含列的部分的集合）上。此函数允许您获取调用其的块的大小。

## 实现(x) {#materializex}

将一个常量列变为一个非常量列。
在ClickHouse中，非常量列和常量列在内存中的表示方式不同。尽管函数对于常量列和非常量总是返回相同的结果，但它们的工作方式可能完全不同（执行不同的代码）。此函数用于调试这种行为。

## ignore(...) {#ignore}

接受任何参数，包括`NULL`。始终返回0。
但是，函数的参数总是被计算的。该函数可以用于基准测试。

## 睡眠（秒) {#sleepseconds}

在每个Block上休眠’seconds’秒。可以是整数或浮点数。

## sleepEachRow（秒) {#sleepeachrowseconds}

在每行上休眠’seconds’秒。可以是整数或浮点数。

## 当前数据库() {#currentdatabase}

返回当前数据库的名称。
当您需要在CREATE TABLE中的表引擎参数中指定数据库，您可以使用此函数。

## isFinite(x) {#isfinitex}

接受Float32或Float64类型的参数，如果参数不是infinite且不是NaN，则返回1，否则返回0。

## isInfinite(x) {#isinfinitex}

接受Float32或Float64类型的参数，如果参数是infinite，则返回1，否则返回0。注意NaN返回0。

## isNaN(x) {#isnanx}

接受Float32或Float64类型的参数，如果参数是Nan，则返回1，否则返回0。

## hasColumnInTable(\[‘hostname’\[, ‘username’\[, ‘password’\]\],\] ‘database’, ‘table’, ‘column’) {#hascolumnintablehostname-username-password-database-table-column}

接受常量字符串：数据库名称、表名称和列名称。 如果存在列，则返回等于1的UInt8常量表达式，否则返回0。 如果设置了hostname参数，则测试将在远程服务器上运行。
如果表不存在，该函数将引发异常。
对于嵌套数据结构中的元素，该函数检查是否存在列。 对于嵌套数据结构本身，函数返回0。

## 酒吧 {#function-bar}

使用unicode构建图表。

`bar(x, min, max, width)` 当`x = max`时， 绘制一个宽度与`(x - min)`成正比且等于`width`的字符带。

参数:

-   `x` — 要显示的尺寸。
-   `min, max` — 整数常量，该值必须是`Int64`。
-   `width` — 常量，可以是正整数或小数。

字符带的绘制精度是符号的八分之一。

示例:

``` sql
SELECT
    toHour(EventTime) AS h,
    count() AS c,
    bar(c, 0, 600000, 20) AS bar
FROM test.hits
GROUP BY h
ORDER BY h ASC
```

    ┌──h─┬──────c─┬─bar────────────────┐
    │  0 │ 292907 │ █████████▋         │
    │  1 │ 180563 │ ██████             │
    │  2 │ 114861 │ ███▋               │
    │  3 │  85069 │ ██▋                │
    │  4 │  68543 │ ██▎                │
    │  5 │  78116 │ ██▌                │
    │  6 │ 113474 │ ███▋               │
    │  7 │ 170678 │ █████▋             │
    │  8 │ 278380 │ █████████▎         │
    │  9 │ 391053 │ █████████████      │
    │ 10 │ 457681 │ ███████████████▎   │
    │ 11 │ 493667 │ ████████████████▍  │
    │ 12 │ 509641 │ ████████████████▊  │
    │ 13 │ 522947 │ █████████████████▍ │
    │ 14 │ 539954 │ █████████████████▊ │
    │ 15 │ 528460 │ █████████████████▌ │
    │ 16 │ 539201 │ █████████████████▊ │
    │ 17 │ 523539 │ █████████████████▍ │
    │ 18 │ 506467 │ ████████████████▊  │
    │ 19 │ 520915 │ █████████████████▎ │
    │ 20 │ 521665 │ █████████████████▍ │
    │ 21 │ 542078 │ ██████████████████ │
    │ 22 │ 493642 │ ████████████████▍  │
    │ 23 │ 400397 │ █████████████▎     │
    └────┴────────┴────────────────────┘

## 变换 {#transform}

根据定义，将某些元素转换为其他元素。
此函数有两种使用方式：

1.  `transform(x, array_from, array_to, default)`

`x` – 要转换的值。

`array_from` – 用于转换的常量数组。

`array_to` – 将’from’中的值转换为的常量数组。

`default` – 如果’x’不等于’from’中的任何值，则默认转换的值。

`array_from` 和 `array_to` – 拥有相同大小的数组。

类型约束:

`transform(T, Array(T), Array(U), U) -> U`

`T`和`U`可以是String，Date，DateTime或任意数值类型的。
对于相同的字母（T或U），如果数值类型，那么它们不可不完全匹配的，只需要具备共同的类型即可。
例如，第一个参数是Int64类型，第二个参数是Array(UInt16)类型。

如果’x’值等于’array_from’数组中的一个元素，它将从’array_to’数组返回一个对应的元素（下标相同）。否则，它返回’default’。如果’array_from’匹配到了多个元素，则返回第一个匹配的元素。

示例:

``` sql
SELECT
    transform(SearchEngineID, [2, 3], ['Yandex', 'Google'], 'Other') AS title,
    count() AS c
FROM test.hits
WHERE SearchEngineID != 0
GROUP BY title
ORDER BY c DESC
```

    ┌─title─────┬──────c─┐
    │ Yandex    │ 498635 │
    │ Google    │ 229872 │
    │ Other     │ 104472 │
    └───────────┴────────┘

1.  `transform(x, array_from, array_to)`

与第一种不同在于省略了’default’参数。
如果’x’值等于’array_from’数组中的一个元素，它将从’array_to’数组返回相应的元素（下标相同）。 否则，它返回’x’。

类型约束:

`transform(T, Array(T), Array(T)) -> T`

示例:

``` sql
SELECT
    transform(domain(Referer), ['yandex.ru', 'google.ru', 'vkontakte.ru'], ['www.yandex', 'example.com', 'vk.com']) AS s,
    count() AS c
FROM test.hits
GROUP BY domain(Referer)
ORDER BY count() DESC
LIMIT 10
```

    ┌─s──────────────┬───────c─┐
    │                │ 2906259 │
    │ www.yandex     │  867767 │
    │ ███████.ru     │  313599 │
    │ mail.yandex.ru │  107147 │
    │ ██████.ru      │  100355 │
    │ █████████.ru   │   65040 │
    │ news.yandex.ru │   64515 │
    │ ██████.net     │   59141 │
    │ example.com    │   57316 │
    └────────────────┴─────────┘

## formatReadableSize(x) {#formatreadablesizex}

接受大小（字节数）。返回带有后缀（KiB, MiB等）的字符串。

示例:

``` sql
SELECT
    arrayJoin([1, 1024, 1024*1024, 192851925]) AS filesize_bytes,
    formatReadableSize(filesize_bytes) AS filesize
```

    ┌─filesize_bytes─┬─filesize───┐
    │              1 │ 1.00 B     │
    │           1024 │ 1.00 KiB   │
    │        1048576 │ 1.00 MiB   │
    │      192851925 │ 183.92 MiB │
    └────────────────┴────────────┘

## 至少(a,b) {#leasta-b}

返回a和b中的最小值。

## 最伟大(a,b) {#greatesta-b}

返回a和b的最大值。

## 碌莽禄time拢time() {#uptime}

返回服务正常运行的秒数。

## 版本() {#version}

以字符串形式返回服务器的版本。

## 时区() {#timezone}

返回服务器的时区。

## blockNumber {#blocknumber}

返回行所在的Block的序列号。

## rowNumberInBlock {#function-rownumberinblock}

返回行所在Block中行的序列号。 针对不同的Block始终重新计算。

## rowNumberInAllBlocks() {#rownumberinallblocks}

返回行所在结果集中的序列号。此函数仅考虑受影响的Block。

## 运行差异(x) {#other_functions-runningdifference}

计算数据块中相邻行的值之间的差异。
对于第一行返回0，并为每个后续行返回与前一行的差异。

函数的结果取决于受影响的Block和Block中的数据顺序。
如果使用ORDER BY创建子查询并从子查询外部调用该函数，则可以获得预期结果。

示例:

``` sql
SELECT
    EventID,
    EventTime,
    runningDifference(EventTime) AS delta
FROM
(
    SELECT
        EventID,
        EventTime
    FROM events
    WHERE EventDate = '2016-11-24'
    ORDER BY EventTime ASC
    LIMIT 5
)
```

    ┌─EventID─┬───────────EventTime─┬─delta─┐
    │    1106 │ 2016-11-24 00:00:04 │     0 │
    │    1107 │ 2016-11-24 00:00:05 │     1 │
    │    1108 │ 2016-11-24 00:00:05 │     0 │
    │    1109 │ 2016-11-24 00:00:09 │     4 │
    │    1110 │ 2016-11-24 00:00:10 │     1 │
    └─────────┴─────────────────────┴───────┘

## 运行差异启动与第一值 {#runningdifferencestartingwithfirstvalue}

与[运行差异](./other-functions.md#other_functions-runningdifference)相同，区别在于第一行返回第一行的值，后续每个后续行返回与上一行的差值。

## MACNumToString(num) {#macnumtostringnum}

接受一个UInt64类型的数字。 将其解释为big endian的MAC地址。 返回包含相应MAC地址的字符串，格式为AA:BB:CC:DD:EE:FF（以冒号分隔的十六进制形式的数字）。

## MACStringToNum(s) {#macstringtonums}

与MACNumToString相反。 如果MAC地址格式无效，则返回0。

## MACStringToOUI(s) {#macstringtoouis}

接受格式为AA:BB:CC:DD:EE:FF（十六进制形式的冒号分隔数字）的MAC地址。 返回前三个八位字节作为UInt64编号。 如果MAC地址格式无效，则返回0。

## getSizeOfEnumType {#getsizeofenumtype}

返回[枚举](../../sql-reference/functions/other-functions.md)中的枚举数量。

    getSizeOfEnumType(value)

**参数:**

-   `value` — `Enum`类型的值。

**返回值**

-   `Enum`的枚举数量。
-   如果类型不是`Enum`，则抛出异常。

**示例**

    SELECT getSizeOfEnumType( CAST('a' AS Enum8('a' = 1, 'b' = 2) ) ) AS x

    ┌─x─┐
    │ 2 │
    └───┘

## toColumnTypeName {#tocolumntypename}

返回在RAM中列的数据类型的名称。

    toColumnTypeName(value)

**参数:**

-   `value` — 任何类型的值。

**返回值**

-   一个字符串，其内容是`value`在RAM中的类型名称。

**`toTypeName ' 与 ' toColumnTypeName`的区别示例**

```sql
SELECT toTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))
```

```response
┌─toTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime                                            │
└─────────────────────────────────────────────────────┘
```

```sql
SELECT toColumnTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))
```

```response
┌─toColumnTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ Const(UInt32)                                             │
└───────────────────────────────────────────────────────────┘
```

该示例显示`DateTime`数据类型作为`Const(UInt32)`存储在内存中。

## dumpColumnStructure {#dumpcolumnstructure}

输出在RAM中的数据结果的详细信息。

    dumpColumnStructure(value)

**参数:**

-   `value` — 任何类型的值.

**返回值**

-   一个字符串，其内容是`value`在RAM中的数据结构的详细描述。

**示例**

    SELECT dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))

    ┌─dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
    │ DateTime, Const(size = 1, UInt32(size = 1))                  │
    └──────────────────────────────────────────────────────────────┘

## defaultValueOfArgumentType {#defaultvalueofargumenttype}

输出数据类型的默认值。

不包括用户设置的自定义列的默认值。

    defaultValueOfArgumentType(expression)

**参数:**

-   `expression` — 任意类型的值或导致任意类型值的表达式。

**返回值**

-   数值类型返回`0`。
-   字符串类型返回空的字符串。
-   [可为空](../../sql-reference/functions/other-functions.md)类型返回`ᴺᵁᴸᴸ`。

**示例**

```sql
SELECT defaultValueOfArgumentType(CAST(1, 'Int8'))
```

```response
┌─defaultValueOfArgumentType(CAST(1, 'Int8'))─┐
│                                           0 │
└─────────────────────────────────────────────┘
```

```sql
SELECT defaultValueOfArgumentType(CAST(1, 'Nullable(Int8)'))
```

```response
┌─defaultValueOfArgumentType(CAST(1, 'Nullable(Int8)'))─┐
│                                                  ᴺᵁᴸᴸ │
└───────────────────────────────────────────────────────┘
```

## indexHint  {#indexhint}
输出符合索引选择范围内的所有数据，同时不实用参数中的表达式进行过滤。

传递给函数的表达式参数将不会被计算，但ClickHouse使用参数中的表达式进行索引过滤。

**返回值**

- 1。

**示例**

这是一个包含[ontime](../../getting-started/example-datasets/ontime.md)测试数据集的测试表。

```
SELECT count() FROM ontime
```
```response
┌─count()─┐
│ 4276457 │
└─────────┘
```

该表使用`(FlightDate, (Year, FlightDate))`作为索引。

对该表进行如下的查询：

```sql
SELECT FlightDate AS k, count() FROM ontime GROUP BY k ORDER BY k
```

```response
SELECT
    FlightDate AS k,
    count()
FROM ontime
GROUP BY k
ORDER BY k ASC

┌──────────k─┬─count()─┐
│ 2017-01-01 │   13970 │
│ 2017-01-02 │   15882 │
........................
│ 2017-09-28 │   16411 │
│ 2017-09-29 │   16384 │
│ 2017-09-30 │   12520 │
└────────────┴─────────┘

273 rows in set. Elapsed: 0.072 sec. Processed 4.28 million rows, 8.55 MB (59.00 million rows/s., 118.01 MB/s.)
```

在这个查询中，由于没有使用索引，所以ClickHouse将处理整个表的所有数据(`Processed 4.28 million rows`)。使用下面的查询尝试使用索引进行查询：

```sql
SELECT FlightDate AS k, count() FROM ontime WHERE k = '2017-09-15' GROUP BY k ORDER BY k
```

```response
SELECT
    FlightDate AS k,
    count()
FROM ontime
WHERE k = '2017-09-15'
GROUP BY k
ORDER BY k ASC

┌──────────k─┬─count()─┐
│ 2017-09-15 │   16428 │
└────────────┴─────────┘

1 rows in set. Elapsed: 0.014 sec. Processed 32.74 thousand rows, 65.49 KB (2.31 million rows/s., 4.63 MB/s.)
```

在最后一行的显示中，通过索引ClickHouse处理的行数明显减少（`Processed 32.74 thousand rows`）。

现在将表达式`k = '2017-09-15'`传递给`indexHint`函数：

```sql
SELECT FlightDate AS k, count() FROM ontime WHERE indexHint(k = '2017-09-15') GROUP BY k ORDER BY k
```

```response
SELECT
    FlightDate AS k,
    count()
FROM ontime
WHERE indexHint(k = '2017-09-15')
GROUP BY k
ORDER BY k ASC

┌──────────k─┬─count()─┐
│ 2017-09-14 │    7071 │
│ 2017-09-15 │   16428 │
│ 2017-09-16 │    1077 │
│ 2017-09-30 │    8167 │
└────────────┴─────────┘

4 rows in set. Elapsed: 0.004 sec. Processed 32.74 thousand rows, 65.49 KB (8.97 million rows/s., 17.94 MB/s.)
```

对于这个请求，根据ClickHouse显示ClickHouse与上一次相同的方式应用了索引（`Processed 32.74 thousand rows`）。但是，最终返回的结果集中并没有根据`k = '2017-09-15'`表达式进行过滤结果。

由于ClickHouse中使用稀疏索引，因此在读取范围时（本示例中为相邻日期），"额外"的数据将包含在索引结果中。使用`indexHint`函数可以查看到它们。

## 复制 {#replicate}

使用单个值填充一个数组。

用于[arrayJoin](array-join.md#functions_arrayjoin)的内部实现。

    replicate(x, arr)

**参数:**

-   `arr` — 原始数组。 ClickHouse创建一个与原始数据长度相同的新数组，并用值`x`填充它。
-   `x` — 生成的数组将被填充的值。

**输出**

-   一个被`x`填充的数组。

**示例**

    SELECT replicate(1, ['a', 'b', 'c'])

    ┌─replicate(1, ['a', 'b', 'c'])─┐
    │ [1,1,1]                       │
    └───────────────────────────────┘

## 文件系统可用 {#filesystemavailable}

返回磁盘的剩余空间信息（以字节为单位）。使用配置文件中的path配置评估此信息。

## 文件系统容量 {#filesystemcapacity}

返回磁盘的容量信息，以字节为单位。使用配置文件中的path配置评估此信息。

## 最后聚会 {#function-finalizeaggregation}

获取聚合函数的状态。返回聚合结果（最终状态）。

## 跑累积 {#function-runningaccumulate}

获取聚合函数的状态并返回其具体的值。这是从第一行到当前行的所有行累计的结果。

例如，获取聚合函数的状态（示例runningAccumulate(uniqState(UserID))），对于数据块的每一行，返回所有先前行和当前行的状态合并后的聚合函数的结果。
因此，函数的结果取决于分区中数据块的顺序以及数据块中行的顺序。

## joinGet(‘join_storage_table_name’, ‘get_column’,join_key) {#joingetjoin-storage-table-name-get-column-join-key}

使用指定的连接键从Join类型引擎的表中获取数据。

## throwIf(x) {#throwifx}

如果参数不为零则抛出异常。
