---
machine_translated: true
machine_translated_rev: b111334d6614a02564cf32f379679e9ff970d9b1
toc_priority: 31
toc_title: "\u8BED\u6CD5"
---

# 语法 {#syntax}

系统中有两种类型的解析器：完整SQL解析器（递归下降解析器）和数据格式解析器（快速流解析器）。
在所有情况下，除了 `INSERT` 查询时，只使用完整的SQL解析器。
该 `INSERT` 查询使用的分析程序:

``` sql
INSERT INTO t VALUES (1, 'Hello, world'), (2, 'abc'), (3, 'def')
```

该 `INSERT INTO t VALUES` 片段由完整的解析器解析，并且数据 `(1, 'Hello, world'), (2, 'abc'), (3, 'def')` 由快速流解析器解析。 您也可以通过使用 [input\_format\_values\_interpret\_expressions](../operations/settings/settings.md#settings-input_format_values_interpret_expressions) 设置。 当 `input_format_values_interpret_expressions = 1`，ClickHouse首先尝试使用fast stream解析器解析值。 如果失败，ClickHouse将尝试对数据使用完整的解析器，将其视为SQL [表达式](#syntax-expressions).

数据可以有任何格式。 当接收到查询时，服务器计算不超过 [max\_query\_size](../operations/settings/settings.md#settings-max_query_size) RAM中请求的字节（默认为1MB），其余的是流解析。
这意味着系统没有大的问题 `INSERT` 查询，就像MySQL一样。

使用时 `Values` 格式为 `INSERT` 查询，它可能看起来数据被解析相同的表达式 `SELECT` 查询，但事实并非如此。 该 `Values` 格式更为有限。

接下来我们将介绍完整的解析器。 有关格式解析器的详细信息，请参阅 [格式](../interfaces/formats.md) 科。

## 空间 {#spaces}

语法结构之间可能有任意数量的空格符号（包括查询的开始和结束）。 空格符号包括空格、制表符、换行符、CR和换页符。

## 评论 {#comments}

支持SQL样式和C样式注释。
SQL风格的评论：来自 `--` 直到最后 后的空间 `--` 可以省略。
C风格的评论：来自 `/*` 到 `*/`. 这些注释可以是多行。 这里也不需要空格。

## 关键词 {#syntax-keywords}

当关键字对应于以下关键字时，不区分大小写:

-   SQL标准。 例如, `SELECT`, `select` 和 `SeLeCt` 都是有效的。
-   在一些流行的DBMS（MySQL或Postgres）中实现。 例如, `DateTime` 是一样的 `datetime`.

数据类型名称是否区分大小写可以在 `system.data_type_families` 桌子

与标准SQL相比，所有其他关键字（包括函数名称）都是 **区分大小写**.

不保留关键字（它们只是在相应的上下文中解析为关键字）。 如果您使用 [标识符](#syntax-identifiers) 与关键字相同，将它们括在引号中。 例如，查询 `SELECT "FROM" FROM table_name` 是有效的，如果表 `table_name` 具有名称的列 `"FROM"`.

## 标识符 {#syntax-identifiers}

标识符是:

-   集群、数据库、表、分区和列名称。
-   功能。
-   数据类型。
-   [表达式别名](#syntax-expression_aliases).

标识符可以是引号或非引号。 建议使用非引号标识符。

非引号标识符必须与正则表达式匹配 `^[a-zA-Z_][0-9a-zA-Z_]*$` 并且不能等于 [关键词](#syntax-keywords). 例: `x, _1, X_y__Z123_.`

如果要使用与关键字相同的标识符，或者要在标识符中使用其他符号，请使用双引号或反引号对其进行引用，例如, `"id"`, `` `id` ``.

## 文字数 {#literals}

有：数字，字符串，复合和 `NULL` 文字。

### 数字 {#numeric}

数值文本尝试进行分析:

-   首先作为一个64位有符号的数字，使用 [strtoull](https://en.cppreference.com/w/cpp/string/byte/strtoul) 功能。
-   如果不成功，作为64位无符号数，使用 [strtoll](https://en.cppreference.com/w/cpp/string/byte/strtol) 功能。
-   如果不成功，作为一个浮点数使用 [strtod](https://en.cppreference.com/w/cpp/string/byte/strtof) 功能。
-   否则，将返回错误。

相应的值将具有该值适合的最小类型。
例如，1被解析为 `UInt8`，但256被解析为 `UInt16`. 有关详细信息，请参阅 [数据类型](../sql-reference/data-types/index.md).

例: `1`, `18446744073709551615`, `0xDEADBEEF`, `01`, `0.1`, `1e100`, `-1e-100`, `inf`, `nan`.

### 字符串 {#syntax-string-literal}

仅支持单引号中的字符串文字。 封闭的字符可以反斜杠转义。 以下转义序列具有相应的特殊值: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\a`, `\v`, `\xHH`. 在所有其他情况下，转义序列的格式为 `\c`，哪里 `c` 是任何字符，被转换为 `c`. 这意味着您可以使用序列 `\'`和`\\`. 该值将具有 [字符串](../sql-reference/data-types/string.md) 类型。

在字符串文字中需要转义的最小字符集: `'` 和 `\`. 单引号可以用单引号，文字转义 `'It\'s'` 和 `'It''s'` 是平等的。

### 化合物 {#compound}

数组支持构造: `[1, 2, 3]` 和元组: `(1, 'Hello, world!', 2)`..
实际上，这些不是文字，而是分别具有数组创建运算符和元组创建运算符的表达式。
数组必须至少包含一个项目，元组必须至少包含两个项目。
元组有一个特殊的用途用于 `IN` a条款 `SELECT` 查询。 元组可以作为查询的结果获得，但它们不能保存到数据库（除了 [记忆](../engines/table-engines/special/memory.md) 表）。

### NULL {#null-literal}

指示该值丢失。

为了存储 `NULL` 在表字段中，它必须是 [可为空](../sql-reference/data-types/nullable.md) 类型。

根据数据格式（输入或输出), `NULL` 可能有不同的表示。 有关详细信息，请参阅以下文档 [数据格式](../interfaces/formats.md#formats).

处理有许多细微差别 `NULL`. 例如，如果比较操作的至少一个参数是 `NULL`，此操作的结果也将是 `NULL`. 对于乘法，加法和其他操作也是如此。 有关详细信息，请阅读每个操作的文档。

在查询中，您可以检查 `NULL` 使用 [IS NULL](operators.md#operator-is-null) 和 [IS NOT NULL](operators.md) 运算符及相关功能 `isNull` 和 `isNotNull`.

## 功能 {#functions}

函数像标识符一样写入，并在括号中包含一个参数列表（可能是空的）。 与标准SQL相比，括号是必需的，即使是空的参数列表。 示例: `now()`.
有常规函数和聚合函数（请参阅部分 “Aggregate functions”). 某些聚合函数可以包含括号中的两个参数列表。 示例: `quantile (0.9) (x)`. 这些聚合函数被调用 “parametric” 函数，并在第一个列表中的参数被调用 “parameters”. 不带参数的聚合函数的语法与常规函数的语法相同。

## 运营商 {#operators}

在查询解析过程中，运算符会转换为相应的函数，同时考虑它们的优先级和关联性。
例如，表达式 `1 + 2 * 3 + 4` 转化为 `plus(plus(1, multiply(2, 3)), 4)`.

## 数据类型和数据库表引擎 {#data_types-and-database-table-engines}

数据类型和表引擎 `CREATE` 查询的编写方式与标识符或函数相同。 换句话说，它们可能包含也可能不包含括在括号中的参数列表。 有关详细信息，请参阅部分 “Data types,” “Table engines,” 和 “CREATE”.

## 表达式别名 {#syntax-expression_aliases}

别名是查询中表达式的用户定义名称。

``` sql
expr AS alias
```

-   `AS` — The keyword for defining aliases. You can define the alias for a table name or a column name in a `SELECT` 子句不使用 `AS` 关键字。

        For example, `SELECT table_name_alias.column_name FROM table_name table_name_alias`.

        In the [CAST](sql_reference/functions/type_conversion_functions.md#type_conversion_function-cast) function, the `AS` keyword has another meaning. See the description of the function.

-   `expr` — Any expression supported by ClickHouse.

        For example, `SELECT column_name * 2 AS double FROM some_table`.

-   `alias` — Name for `expr`. 别名应符合 [标识符](#syntax-identifiers) 语法

        For example, `SELECT "table t".column_name FROM table_name AS "table t"`.

### 使用注意事项 {#notes-on-usage}

别名对于查询或子查询是全局的，您可以在查询的任何部分中为任何表达式定义别名。 例如, `SELECT (1 AS n) + 2, n`.

别名在子查询和子查询之间不可见。 例如，在执行查询时 `SELECT (SELECT sum(b.a) + num FROM b) - a.a AS num FROM a` ClickHouse生成异常 `Unknown identifier: num`.

如果为结果列定义了别名 `SELECT` 子查询的子句，这些列在外部查询中可见。 例如, `SELECT n + m FROM (SELECT 1 AS n, 2 AS m)`.

小心使用与列或表名相同的别名。 让我们考虑以下示例:

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

在这个例子中，我们声明表 `t` 带柱 `b`. 然后，在选择数据时，我们定义了 `sum(b) AS b` 别名 由于别名是全局的，ClickHouse替换了文字 `b` 在表达式中 `argMax(a, b)` 用表达式 `sum(b)`. 这种替换导致异常。

## 星号 {#asterisk}

在一个 `SELECT` 查询中，星号可以替换表达式。 有关详细信息，请参阅部分 “SELECT”.

## 表达式 {#syntax-expressions}

表达式是函数、标识符、文字、运算符的应用程序、括号中的表达式、子查询或星号。 它还可以包含别名。
表达式列表是一个或多个用逗号分隔的表达式。
函数和运算符，反过来，可以有表达式作为参数。

[原始文章](https://clickhouse.tech/docs/en/query_language/syntax/) <!--hide-->
