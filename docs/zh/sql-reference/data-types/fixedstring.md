---
slug: /zh/sql-reference/data-types/fixedstring
---
# 固定字符串 {#fixedstring}

固定长度 N 的字符串（N 必须是严格的正自然数）。

您可以使用下面的语法对列声明为`FixedString`类型：

``` sql
<column_name> FixedString(N)
```

其中`N`表示自然数。

当数据的长度恰好为N个字节时，`FixedString`类型是高效的。 在其他情况下，这可能会降低效率。

可以有效存储在`FixedString`类型的列中的值的示例：

-   二进制表示的IP地址（IPv6使用`FixedString(16)`）
-   语言代码（ru_RU, en_US ... ）
-   货币代码（USD, RUB ... ）
-   二进制表示的哈希值（MD5使用`FixedString(16)`，SHA256使用`FixedString(32)`）

请使用[UUID](uuid.md)数据类型来存储UUID值，。

当向ClickHouse中插入数据时,

-   如果字符串包含的字节数少于\`N’,将对字符串末尾进行空字节填充。
-   如果字符串包含的字节数大于`N`,将抛出`Too large value for FixedString(N)`异常。

当做数据查询时，ClickHouse不会删除字符串末尾的空字节。 如果使用`WHERE`子句，则须要手动添加空字节以匹配`FixedString`的值。 以下示例阐明了如何将`WHERE`子句与`FixedString`一起使用。

考虑带有`FixedString（2)`列的表：

``` text
┌─name──┐
│ b     │
└───────┘
```

查询语句`SELECT * FROM FixedStringTable WHERE a = 'b'` 不会返回任何结果。请使用空字节来填充筛选条件。

``` sql
SELECT * FROM FixedStringTable
WHERE a = 'b\0'
```

``` text
┌─a─┐
│ b │
└───┘
```

这种方式与MySQL的`CHAR`类型的方式不同（MySQL中使用空格填充字符串，并在输出时删除空格）。

请注意，`FixedString(N)`的长度是个常量。仅由空字符组成的字符串，函数[length](../../sql-reference/functions/array-functions.md#array_functions-length)返回值为`N`,而函数[empty](../../sql-reference/functions/string-functions.md#empty)的返回值为`1`。
