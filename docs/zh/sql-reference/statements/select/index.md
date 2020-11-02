---
title: SELECT Query
toc_folder_title: SELECT
toc_priority: 33
toc_title: 综述
---

# 选择查询 {#select-queries-syntax}

`SELECT` 查询执行数据检索。 默认情况下，请求的数据返回给客户端，同时结合 [INSERT INTO](../../../sql-reference/statements/insert-into.md) 可以被转发到不同的表。

## 语法 {#syntax}

``` sql
[WITH expr_list|(subquery)]
SELECT [DISTINCT] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI] JOIN (subquery)|table (ON <expr_list>)|(USING <column_list>)
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH TOTALS]
[HAVING expr]
[ORDER BY expr_list] [WITH FILL] [FROM expr] [TO expr] [STEP expr] 
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m] [WITH TIES]
[UNION ALL ...]
[INTO OUTFILE filename]
[FORMAT format]
```

所有子句都是可选的，但紧接在 `SELECT` 后面的必需表达式列表除外，更详细的请看 [下面](#select-clause).

每个可选子句的具体内容在单独的部分中进行介绍，这些部分按与执行顺序相同的顺序列出:

-   [WITH 子句](../../../sql-reference/statements/select/with.md)
-   [FROM 子句](../../../sql-reference/statements/select/from.md)
-   [SAMPLE 子句](../../../sql-reference/statements/select/sample.md)
-   [JOIN 子句](../../../sql-reference/statements/select/join.md)
-   [PREWHERE 子句](../../../sql-reference/statements/select/prewhere.md)
-   [WHERE 子句](../../../sql-reference/statements/select/where.md)
-   [GROUP BY 子句](../../../sql-reference/statements/select/group-by.md)
-   [LIMIT BY 子句](../../../sql-reference/statements/select/limit-by.md)
-   [HAVING 子句](../../../sql-reference/statements/select/having.md)
-   [SELECT 子句](#select-clause)
-   [DISTINCT 子句](../../../sql-reference/statements/select/distinct.md)
-   [LIMIT 子句](../../../sql-reference/statements/select/limit.md)
-   [UNION ALL 子句](../../../sql-reference/statements/select/union-all.md)
-   [INTO OUTFILE 子句](../../../sql-reference/statements/select/into-outfile.md)
-   [FORMAT 子句](../../../sql-reference/statements/select/format.md)

## SELECT 子句 {#select-clause}

[表达式](../../../sql-reference/syntax.md#syntax-expressions) 指定 `SELECT` 子句是在上述子句中的所有操作完成后计算的。 这些表达式的工作方式就好像它们应用于结果中的单独行一样。 如果表达式 `SELECT` 子句包含聚合函数，然后ClickHouse将使用 [GROUP BY](../../../sql-reference/statements/select/group-by.md) 聚合参数应用在聚合函数和表达式上。

如果在结果中包含所有列，请使用星号 (`*`）符号。 例如, `SELECT * FROM ...`.

将结果中的某些列与 [re2](https://en.wikipedia.org/wiki/RE2_(software)) 正则表达式匹配，可以使用 `COLUMNS` 表达。

``` sql
COLUMNS('regexp')
```

例如表:

``` sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

以下查询所有列名包含 `a` 。

``` sql
SELECT COLUMNS('a') FROM col_names
```

``` text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

所选列不按字母顺序返回。

您可以使用多个 `COLUMNS` 表达式并将函数应用于它们。

例如:

``` sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

``` text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

返回的每一列 `COLUMNS` 表达式作为单独的参数传递给函数。 如果函数支持其他参数，您也可以将其他参数传递给函数。 使用函数时要小心，如果函数不支持传递给它的参数，ClickHouse将抛出异常。

例如:

``` sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

``` text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus doesn't match: passed 3, should be 2.
```

该例子中, `COLUMNS('a')` 返回两列: `aa` 和 `ab`. `COLUMNS('c')` 返回 `bc` 列。 该 `+` 运算符不能应用于3个参数，因此ClickHouse抛出一个带有相关消息的异常。

匹配的列 `COLUMNS` 表达式可以具有不同的数据类型。 如果 `COLUMNS` 不匹配任何列，并且是在 `SELECT` 唯一的表达式，ClickHouse则抛出异常。

### 星号 {#asterisk}

您可以在查询的任何部分使用星号替代表达式。进行查询分析、时，星号将展开为所有表的列（不包括 `MATERIALIZED` 和 `ALIAS` 列）。 只有少数情况下使用星号是合理的:

-   创建转储表时。
-   对于只包含几列的表，例如系统表。
-   获取表中列的信息。 在这种情况下，设置 `LIMIT 1`. 但最好使用 `DESC TABLE` 查询。
-   当对少量列使用 `PREWHERE` 进行强过滤时。
-   在子查询中（因为外部查询不需要的列从子查询中排除）。

在所有其他情况下，我们不建议使用星号，因为它只给你一个列DBMS的缺点，而不是优点。 换句话说，不建议使用星号。

### 极端值 {#extreme-values}

除结果之外，还可以获取结果列的最小值和最大值。 要做到这一点，设置 **extremes** 设置为1。 最小值和最大值是针对数字类型、日期和带有时间的日期计算的。 对于其他类型列，输出默认值。

分别的额外计算两行 – 最小值和最大值。 这额外的两行采用输出格式为 `JSON*`, `TabSeparated*`，和 `Pretty*` [formats](../../../interfaces/formats.md)，与其他行分开。 它们不以其他格式输出。

为 `JSON*` 格式时，极端值单独的输出在 ‘extremes’ 字段。 为 `TabSeparated*` 格式时，此行来的主要结果集后，然后显示 ‘totals’ 字段。 它前面有一个空行（在其他数据之后）。 在 `Pretty*` 格式时，该行在主结果之后输出为一个单独的表，然后显示 ‘totals’ 字段。

极端值在 `LIMIT` 之前被计算，但在 `LIMIT BY` 之后被计算. 然而，使用 `LIMIT offset, size`， `offset` 之前的行都包含在 `extremes`. 在流请求中，结果还可能包括少量通过 `LIMIT` 过滤的行.

### 备注 {#notes}

您可以在查询的任何部分使用同义词 (`AS` 别名）。

 `GROUP BY` 和 `ORDER BY` 子句不支持位置参数。 这与MySQL相矛盾，但符合标准SQL。 例如, `GROUP BY 1, 2` 将被理解为根据常量分组 (i.e. aggregation of all rows into one).

## 实现细节 {#implementation-details}

如果查询省略 `DISTINCT`, `GROUP BY` ， `ORDER BY` ， `IN` ， `JOIN` 子查询，查询将被完全流处理，使用O(1)量的RAM。 若未指定适当的限制，则查询可能会消耗大量RAM:

-   `max_memory_usage`
-   `max_rows_to_group_by`
-   `max_rows_to_sort`
-   `max_rows_in_distinct`
-   `max_bytes_in_distinct`
-   `max_rows_in_set`
-   `max_bytes_in_set`
-   `max_rows_in_join`
-   `max_bytes_in_join`
-   `max_bytes_before_external_sort`
-   `max_bytes_before_external_group_by`

有关详细信息，请参阅部分 “Settings”. 可以使用外部排序（将临时表保存到磁盘）和外部聚合。

{## [原始文章](https://clickhouse.tech/docs/en/sql-reference/statements/select/) ##}
