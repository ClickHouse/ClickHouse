---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
title: SELECT Query
toc_folder_title: SELECT
toc_priority: 33
toc_title: "\u6982\u8FF0"
---

# 选择查询 {#select-queries-syntax}

`SELECT` 查询执行数据检索。 默认情况下，请求的数据返回给客户端，同时与 [INSERT INTO](../../../sql-reference/statements/insert-into.md) 它可以被转发到不同的表。

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

所有子句都是可选的，但紧接在后面的必需表达式列表除外 `SELECT` 这是更详细的复盖 [下面](#select-clause).

每个可选子句的具体内容在单独的部分中进行了介绍，这些部分按与执行顺序相同的顺序列出:

-   [WITH条款](../../../sql-reference/statements/select/with.md)
-   [FROM条款](../../../sql-reference/statements/select/from.md)
-   [示例子句](../../../sql-reference/statements/select/sample.md)
-   [JOIN子句](../../../sql-reference/statements/select/join.md)
-   [PREWHERE条款](../../../sql-reference/statements/select/prewhere.md)
-   [WHERE条款](../../../sql-reference/statements/select/where.md)
-   [GROUP BY子句](../../../sql-reference/statements/select/group-by.md)
-   [限制条款](../../../sql-reference/statements/select/limit-by.md)
-   [有条款](../../../sql-reference/statements/select/having.md)
-   [SELECT子句](#select-clause)
-   [DISTINCT子句](../../../sql-reference/statements/select/distinct.md)
-   [限制条款](../../../sql-reference/statements/select/limit.md)
-   [UNION ALL条款](../../../sql-reference/statements/select/union-all.md)
-   [INTO OUTFILE条款](../../../sql-reference/statements/select/into-outfile.md)
-   [格式子句](../../../sql-reference/statements/select/format.md)

## SELECT子句 {#select-clause}

[表达式](../../../sql-reference/syntax.md#syntax-expressions) 在指定 `SELECT` 子句是在上述子句中的所有操作完成后计算的。 这些表达式的工作方式就好像它们应用于结果中的单独行一样。 如果在表达式 `SELECT` 子句包含聚合函数，然后ClickHouse处理过程中用作其参数的聚合函数和表达式 [GROUP BY](../../../sql-reference/statements/select/group-by.md) 聚合。

如果要在结果中包含所有列，请使用星号 (`*`）符号。 例如, `SELECT * FROM ...`.

将结果中的某些列与 [re2](https://en.wikipedia.org/wiki/RE2_(software)) 正则表达式，您可以使用 `COLUMNS` 表达。

``` sql
COLUMNS('regexp')
```

例如，考虑表:

``` sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

以下查询从包含以下内容的所有列中选择数据 `a` 在他们的名字符号。

``` sql
SELECT COLUMNS('a') FROM col_names
```

``` text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

所选列不按字母顺序返回。

您可以使用多个 `COLUMNS` 查询中的表达式并将函数应用于它们。

例如:

``` sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

``` text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

由返回的每一列 `COLUMNS` 表达式作为单独的参数传递给函数。 如果函数支持其他参数，您也可以将其他参数传递给函数。 使用函数时要小心。 如果函数不支持您传递给它的参数数，ClickHouse将引发异常。

例如:

``` sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

``` text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus doesn't match: passed 3, should be 2.
```

在这个例子中, `COLUMNS('a')` 返回两列: `aa` 和 `ab`. `COLUMNS('c')` 返回 `bc` 列。 该 `+` 运算符不能应用于3个参数，因此ClickHouse引发一个带有相关消息的异常。

匹配的列 `COLUMNS` 表达式可以具有不同的数据类型。 如果 `COLUMNS` 不匹配任何列，并且是唯一的表达式 `SELECT`，ClickHouse抛出异常。

### 星号 {#asterisk}

您可以在查询的任何部分而不是表达式中添加星号。 分析查询时，星号将展开为所有表列的列表（不包括 `MATERIALIZED` 和 `ALIAS` 列）。 只有少数情况下使用星号是合理的:

-   创建表转储时。
-   对于只包含几列的表，例如系统表。
-   获取有关表中哪些列的信息。 在这种情况下，设置 `LIMIT 1`. 但最好使用 `DESC TABLE` 查询。
-   当对少量柱进行强过滤时，使用 `PREWHERE`.
-   在子查询中（因为外部查询不需要的列从子查询中排除）。

在所有其他情况下，我们不建议使用星号，因为它只给你一个列DBMS的缺点，而不是优点。 换句话说，不建议使用星号。

### 极端值 {#extreme-values}

除了结果之外，还可以获取结果列的最小值和最大值。 要做到这一点，设置 **极端** 设置为1。 最小值和最大值是针对数字类型、日期和带有时间的日期计算的。 对于其他列，默认值为输出。

An extra two rows are calculated – the minimums and maximums, respectively. These extra two rows are output in `JSON*`, `TabSeparated*`，和 `Pretty*` [格式](../../../interfaces/formats.md)，与其他行分开。 它们不是其他格式的输出。

在 `JSON*` 格式时，极端值在一个单独的输出 ‘extremes’ 场。 在 `TabSeparated*` 格式中，该行来的主要结果之后，和之后 ‘totals’ 如果存在。 它前面有一个空行（在其他数据之后）。 在 `Pretty*` 格式中，该行被输出为一个单独的表之后的主结果，和之后 `totals` 如果存在。

极值计算之前的行 `LIMIT`，但之后 `LIMIT BY`. 但是，使用时 `LIMIT offset, size`，之前的行 `offset` 都包含在 `extremes`. 在流请求中，结果还可能包括少量通过的行 `LIMIT`.

### 注 {#notes}

您可以使用同义词 (`AS` 别名）在查询的任何部分。

该 `GROUP BY` 和 `ORDER BY` 子句不支持位置参数。 这与MySQL相矛盾，但符合标准SQL。 例如, `GROUP BY 1, 2` will be interpreted as grouping by constants (i.e. aggregation of all rows into one).

## 实施细节 {#implementation-details}

如果查询省略 `DISTINCT`, `GROUP BY` 和 `ORDER BY` 条款和 `IN` 和 `JOIN` 子查询，查询将被完全流处理，使用O(1)量的RAM。 否则，如果未指定适当的限制，则查询可能会消耗大量RAM:

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
