---
toc_priority: 31
toc_title: SQL语法

---

# SQL语法 {#syntax}

ClickHouse有2类解析器：完整SQL解析器（递归式解析器），以及数据格式解析器（快速流式解析器）
除了 `INSERT` 查询，其它情况下仅使用完整SQL解析器。
 `INSERT`查询会同时使用2种解析器：

``` sql
INSERT INTO t VALUES (1, 'Hello, world'), (2, 'abc'), (3, 'def')
```

含`INSERT INTO t VALUES` 的部分由完整SQL解析器处理，包含数据的部分 `(1, 'Hello, world'), (2, 'abc'), (3, 'def')` 交给快速流式解析器解析。通过设置参数 [input_format_values_interpret_expressions](../operations/settings/settings.md#settings-input_format_values_interpret_expressions)，你也可以对数据部分开启完整SQL解析器。当 `input_format_values_interpret_expressions = 1` 时，ClickHouse优先采用快速流式解析器来解析数据。如果失败，ClickHouse再尝试用完整SQL解析器来处理，就像处理SQL [expression](#syntax-expressions) 一样。

数据可以采用任何格式。当CH接收到请求时，服务端先在内存中计算不超过 [max_query_size](../operations/settings/settings.md#settings-max_query_size) 字节的请求数据（默认1 mb），然后剩下部分交给快速流式解析器。

当 `INSERT` 语句中使用 `Values` 格式时，看起来数据部分的解析和解析`SELECT` 中的表达式相同，但并不是这样的。 `Values` 格式有非常多的限制。

本文的剩余部分涵盖了完整SQL解析器。关于格式解析的更多信息，参见 [Formats](../interfaces/formats.md) 章节。

## 空白{#spaces}

sql语句的语法结构部分之间（标识符之间、部分符号之间、包括sql的起始和结束）可以有任意的空白字符，这些空字符类型包括：空格字符，tab制表符，换行符，CR符，换页符等。

## 注释 {#comments}

ClickHouse支持SQL风格或C语言风格的注释：

- SQL风格的注释以 `--` 开始，直到行末，`--` 后紧跟的空格可以忽略
- C语言风格的注释以 `/*` 开始，以 `*/` 结束，可以跨行，同样可以省略 `/*` 后的空格

## 关键字 {#syntax-keywords}

以下场景的关键字是大小写不敏感的：

- 标准SQL。例如，`SELECT`, `select` 和 `SeLeCt` 都是允许的
- 在某些流行的RDBMS中被实现的关键字，例如，`DateTime` 和 `datetime`是一样的


你可以在系统表 [system.data_type_families](../operations/system-tables/data_type_families.md#system_tables-data_type_families) 中检查某个数据类型的名称是否是大小写敏感型。

和标准SQL相反，所有其它的关键字都是 **大小写敏感的**，包括函数名称。

关键字不是保留的；它们仅在相应的上下文中才会被认为是关键字。如果你使用和关键字同名的 [标识符](#syntax-identifiers) ，需要使用双引号或反引号将它们包含起来。例如：如果表 `table_name` 包含列 `"FROM"`，那么 `SELECT "FROM" FROM table_name` 是合法的

## 标识符 {#syntax-identifiers}

标识符包括：

-   集群、数据库、表、分区、列的名称
-   函数
-   数据类型
-   [表达式别名](https://clickhouse.tech/docs/zh/sql-reference/syntax/#syntax-expression_aliases)

变量名可以被括起或不括起，后者是推荐做法。

没有括起的变量名，必须匹配正则表达式 `^[a-zA-Z_][0-9a-zA-Z_]*$`，并且不能和 [关键字](#syntax-keywords)相同，合法的标识符名称：`x`，`_1`，`X_y__Z123_`等。

如果想使用和关键字同名的变量名称，或者在变量名称中包含其它符号，你需要通过双引号或反引号，例如： `"id"`, `` `id` ``

## 字符 {#literals}

字符包含数字，字母，括号，NULL值等字符。

### 数字 {#numeric}

数字类型字符会被做如下解析：

-   首先，当做64位的有符号整数，使用函数 [strtoull](https://en.cppreference.com/w/cpp/string/byte/strtoul)
-   如果失败，解析成64位无符号整数，同样使用函数 [strtoull](https://en.cppreference.com/w/cpp/string/byte/strtoul)

-   如果还失败了，试图解析成浮点型数值，使用函数 [strtod](https://en.cppreference.com/w/cpp/string/byte/strtof)

-   最后，以上情形都不符合时，返回异常


数字类型的值类型为能容纳该值的最小数据类型。
例如：1 解析成 `UInt8`型，256 则解析成 `UInt16`。更多信息，参见 [数据类型](../sql-reference/data-types/index.md)

例如: `1`, `18446744073709551615`, `0xDEADBEEF`, `01`, `0.1`, `1e100`, `-1e-100`, `inf`, `nan`.

### 字符串 {#syntax-string-literal}

ClickHouse只支持用单引号包含的字符串。特殊字符可通过反斜杠进行转义。下列转义字符都有相应的实际值： `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\a`, `\v`, `\xHH`。其它情况下，以 `\c`形式出现的转义字符，当`c`表示任意字符时，转义字符会转换成`c`。这意味着你可以使用 `\'`和`\\`。该值将拥有[String](../sql-reference/data-types/string.md)类型。


在字符串中，你至少需要对 `'` 和 `\` 进行转义。单引号可以使用单引号转义，例如 `'It\'s'` 和 `'It''s'` 是相同的。

### 复合字符串 {#compound}

数组都是使用方括号进行构造 `[1, 2, 3]`，元组则使用圆括号 `(1, 'Hello, world!', 2)`
从技术上来讲，这些都不是字符串，而是包含创建数组和元组运算符的表达式。
创建一个数组必须至少包含一个元素，创建一个元组至少包含2个元素
当元组出现在 `SELECT` 查询的 `IN` 部分时，是一种例外情形。查询结果可以包含元组，但是元组类型不能保存到数据库中（除非表采用 [内存表](../engines/table-engines/special/memory.md)引擎）

### NULL值 {#null-literal}

代表不存在的值。

为了能在表字段中存储NULL值，该字段必须声明为 [空值](../sql-reference/data-types/nullable.md) 类型。
根据数据的格式（输入或输出），NULL值有不同的表现形式。更多信息参见文档 [数据格式](../interfaces/formats.md#formats)

在处理 `NULL`时存在很多细微差别。例如，比较运算的至少一个参数为 `NULL` ，则该结果也是 `NULL` 。与之类似的还有乘法运算, 加法运算,以及其它运算。更多信息，请参阅每种运算的文档部分。

在语句中，可以通过 [IS NULL](operators/index.md#operator-is-null) 以及 [IS NOT NULL](operators/index.md) 运算符，以及 `isNull` 、 `isNotNull` 函数来检查 `NULL` 值

## 函数 {#functions}

函数调用的写法，类似于一个标识符后接被圆括号包含的参数列表（可能为空）。与标准SQL不同，圆括号是必须的，不管参数列表是否为空。例如： `now()`。

函数分为常规函数和聚合函数（参见“Aggregate functions”一章）。有些聚合函数包含2个参数列表，第一个参数列表中的参数被称为“parameters”。不包含“parameters”的聚合函数语法和常规函数是一样的。


## 运算符 {#operators}

在查询解析阶段，运算符会被转换成对应的函数，使用时请注意它们的优先级。例如：
表达式 `1 + 2 * 3 + 4` 会被解析成 `plus(plus(1, multiply(2, 3)), 4)`.


## 数据类型及数据库/表引擎 {#data_types-and-database-table-engines}

`CREATE` 语句中的数据类型和表引擎写法与变量或函数类似。
换句话说，它们可以包含或不包含用括号包含的参数列表。更多信息，参见“数据类型,” “数据表引擎” 和 “CREATE语句”等章节

## 表达式别名 {#syntax-expression_aliases}

别名是用户对表达式的自定义名称

``` sql
expr AS alias
```

- `AS` — 用于定义别名的关键字。可以对表或select语句中的列定义别名(`AS` 可以省略）
  例如, `SELECT table_name_alias.column_name FROM table_name table_name_alias`.

  在 [CAST函数](../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) 中，`AS`有其它含义。请参见该函数的说明部分。


- `expr` — 任意CH支持的表达式.

  例如, `SELECT column_name * 2 AS double FROM some_table`.

- `alias` —  `expr` 的名称。别名必须符合 [标识符](#syntax-identifiers) 语法.

  例如, `SELECT "table t".column_name FROM table_name AS "table t"`.

### 用法注意 {#notes-on-usage}

别名在当前查询或子查询中是全局可见的，你可以在查询语句的任何位置对表达式定义别名

别名在当前查询的子查询及不同子查询中是不可见的。例如，执行如下查询SQL: `SELECT (SELECT sum(b.a) + num FROM b) - a.a AS num FROM a` ,ClickHouse会提示异常 `Unknown identifier: num`.

如果给select子查询语句的结果列定义其别名，那么在外层可以使用该别名。例如, `SELECT n + m FROM (SELECT 1 AS n, 2 AS m)`.

注意列的别名和表的别名相同时的情形，考虑如下示例：

``` sql
CREATE TABLE t
(
    a Int,
    b Int
)
ENGINE = TinyLog()
```

``` sql
SELECT
    argMax(a, b),
    sum(b) AS b
FROM t
```

``` text
Received exception from server (version 18.14.17):
Code: 184. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: Aggregate function sum(b) is found inside another aggregate function in query.
```

在这个示例中，先声明了表 `t` 以及列 `b`。然后，在查询数据时，又定义了别名 `sum(b) AS b`。由于别名是全局的，ClickHouse使用表达式 `sum(b)` 来替换表达式 `argMax(a, b)` 中的变量 `b`。这种替换导致出现异常。

## 星号 {#asterisk}

select查询中，星号可以代替表达式使用。详情请参见“select”部分


## 表达式 {#syntax-expressions}

表达式是函数、标识符、字符、使用运算符的语句、括号中的表达式、子查询或星号。它也可以包含别名。
表达式列表是用逗号分隔的一个或多个表达式。
反过来，函数和运算符可以将表达式作为参数。

[原始文档](https://clickhouse.tech/docs/en/sql_reference/syntax/) <!--hide-->
