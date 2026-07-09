---
description: 'SELECT 查询文档'
sidebar_label: 'SELECT'
sidebar_position: 32
slug: /sql-reference/statements/select/
title: 'SELECT 查询'
doc_type: 'reference'
---

`SELECT` 查询用于检索数据。默认情况下，请求的数据会返回给客户端；而与 [INSERT INTO](../../../sql-reference/statements/insert-into.md) 搭配使用时，查询结果可转发到另一张表。

<div id="syntax">
  ## 语法
</div>

```sql
[WITH expr_list(subquery)]
SELECT [DISTINCT [ON (column1, column2, ...)]] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI] JOIN (subquery)|table [(alias1 [, alias2 ...])] (ON <expr_list>)|(USING <column_list>)
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH ROLLUP|WITH CUBE] [WITH TOTALS]
[HAVING expr]
[WINDOW window_expr_list]
[QUALIFY expr]
[ORDER BY expr_list] [WITH FILL] [FROM expr] [TO expr] [STEP expr] [INTERPOLATE [(expr_list)]]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m] [WITH TIES]
[SETTINGS ...]
[UNION  ...]
[INTO OUTFILE filename [TRUNCATE] [COMPRESSION type [LEVEL level]] ]
[FORMAT format]
```

除 `SELECT` 后紧接着的必需表达式列表外，所有子句都是可选的；这一部分将在[下文](#select-clause)中进一步详细说明。

各可选子句的具体说明分别见独立章节，并按其执行顺序列出：

* [WITH 子句](../../../sql-reference/statements/select/with.md)
* [SELECT 子句](#select-clause)
* [DISTINCT 子句](../../../sql-reference/statements/select/distinct.md)
* [FROM 子句](../../../sql-reference/statements/select/from.md)
* [SAMPLE 子句](../../../sql-reference/statements/select/sample.md)
* [JOIN 子句](../../../sql-reference/statements/select/join.md)
* [PREWHERE 子句](../../../sql-reference/statements/select/prewhere.md)
* [WHERE 子句](../../../sql-reference/statements/select/where.md)
* [WINDOW 子句](../../../sql-reference/window-functions/index.md)
* [GROUP BY 子句](/zh/sql-reference/statements/select/group-by)
* [LIMIT BY 子句](../../../sql-reference/statements/select/limit-by.md)
* [HAVING 子句](../../../sql-reference/statements/select/having.md)
* [QUALIFY 子句](../../../sql-reference/statements/select/qualify.md)
* [LIMIT 子句](../../../sql-reference/statements/select/limit.md)
* [OFFSET 子句](../../../sql-reference/statements/select/offset.md)
* [UNION 子句](../../../sql-reference/statements/select/union.md)
* [INTERSECT 子句](../../../sql-reference/statements/select/intersect.md)
* [EXCEPT 子句](../../../sql-reference/statements/select/except.md)
* [INTO OUTFILE 子句](../../../sql-reference/statements/select/into-outfile.md)
* [FORMAT 子句](../../../sql-reference/statements/select/format.md)

<div id="select-clause">
  ## SELECT 子句
</div>

在 `SELECT` 子句中指定的[表达式](/zh/sql-reference/syntax#expressions)，会在上述子句中的所有操作完成后计算。这些表达式的作用方式，就像分别应用于结果中的每一行一样。如果 `SELECT` 子句中的表达式包含聚合函数，那么 ClickHouse 会在 [GROUP BY](/zh/sql-reference/statements/select/group-by) 聚合过程中处理这些聚合函数及其参数中使用的表达式。

如果想在结果中包含所有列，请使用星号 (`*`) 。例如，`SELECT * FROM ...`。

<div id="dynamic-column-selection">
  ### 动态列选择
</div>

动态列选择 (也称为 COLUMNS 表达式) 允许你使用 [re2](https://en.wikipedia.org/wiki/RE2_\(software\)) 正则表达式匹配结果中的某些列。

```sql
COLUMNS('regexp')
```

例如，来看下表：

```sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

以下查询会从名称中包含 `a` 字符的所有列里选取数据。

```sql
SELECT COLUMNS('a') FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

返回所选列时，不会按字母顺序排序。

你可以在一个查询中使用多个 `COLUMNS` 表达式，并对其应用函数。

例如：

```sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

```text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

`COLUMNS` 表达式返回的每一列都会作为单独的参数传递给函数。如果函数支持，你也可以向函数传递其他参数。使用函数时请务必小心。如果某个函数不支持你传递给它的参数数量，ClickHouse 会抛出异常。

例如：

```sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

```text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus does not match: passed 3, should be 2.
```

在此示例中，`COLUMNS('a')` 返回两列：`aa` 和 `ab`。`COLUMNS('c')` 返回 `bc` 列。`+` 运算符无法应用于 3 个参数，因此 ClickHouse 会抛出异常并显示相应消息。

与 `COLUMNS` 表达式匹配的列可以具有不同的数据类型。如果 `COLUMNS` 未匹配到任何列，并且它是 `SELECT` 中唯一的表达式，ClickHouse 会抛出异常。

<div id="select-columns-with-like-or-ilike">
  #### 使用 `LIKE` 或 `ILIKE` 选择列
</div>

你也可以在 `*` 之后使用区分大小写的 `LIKE` 或不区分大小写的 `ILIKE`，通过将列名与模式匹配来选择列：

```sql
SELECT * ILIKE 'a%' FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

`LIKE` 和 `ILIKE` 模式遵循 `LIKE` 的语义，而非正则表达式的语义。`%` 字符可匹配任意字符序列，`_` 字符可匹配任意单个字符，`\` 用于转义 `%`、`_` 和 `\`。两者唯一的区别在于：`LIKE` 匹配列名时区分大小写，而 `ILIKE` 不区分大小写。例如：

```sql
SELECT * ILIKE 'a_' FROM col_names
```

该查询会选择名称为两个字符且以 `a` 开头的列，例如 `aa` 和 `ab`。

`* LIKE` 和 `* ILIKE` 也支持限定星号和列转换器：

```sql
SELECT t.* ILIKE 'a%' EXCEPT (ab) FROM col_names AS t
```

```text
┌─aa─┐
│  1 │
└────┘
```

<div id="asterisk">
  ### 星号
</div>

你可以在查询中的任何位置用星号代替表达式。在分析查询时，星号会展开为表中所有列的列表 (不包括 `MATERIALIZED` 和 `ALIAS` 列) 。只有少数几种情况下，使用星号才是合理的：

* 创建表转储时。
* 对于只包含少量列的表，例如系统表。
* 想了解表中有哪些列时。在这种情况下，可设置 `LIMIT 1`。但更好的方式是使用 `DESC TABLE` 查询。
* 使用 `PREWHERE` 对少数列进行强过滤时。
* 在子查询中 (因为外部查询不需要的列会从子查询中排除) 。

除此之外的所有情况，我们都不建议使用星号，因为这样只会带来列式 DBMS 的缺点，而无法发挥其优势。换句话说，不推荐使用星号。

<div id="extreme-values">
  ### 极值
</div>

除了查询结果外，你还可以获取结果各列的最小值和最大值。为此，请将 **extremes** 设置为 1。系统会为数值类型、日期以及日期时间类型计算最小值和最大值。对于其他列，则输出默认值。

系统会额外计算两行，分别表示最小值和最大值。这两行会在 `XML`、`JSON*`、`TabSeparated*`、`CSV*`、`Vertical`、`Template` 和 `Pretty*` [格式](../../../interfaces/formats.md)中与其他行分开输出。其他格式则不会输出这两行。

在 `JSON*` 和 `XML` 格式中，极值会输出到单独的 `extremes` 字段中。在 `TabSeparated*`、`CSV*` 和 `Vertical` 格式中，这一行会出现在主结果之后；如果存在 `totals`，则还会出现在 `totals` 之后。并且在它之前会有一个空行 (位于其他数据之后) 。在 `Pretty*` 格式中，这一行会作为单独的表输出在主结果之后；如果存在 `totals`，则也会出现在其后。在 `Template` 格式中，极值会按照指定模板输出。

极值是基于 `LIMIT` 之前、但 `LIMIT BY` 之后的行计算的。不过，使用 `LIMIT offset, size` 时，`offset` 之前的行也会包含在 `extremes` 中。在流式请求中，结果中也可能包含少量通过 `LIMIT` 的行。

<div id="notes">
  ### 说明
</div>

你可以在查询的任何部分使用同义名 (`AS` 别名) 。

`GROUP BY`、`ORDER BY` 和 `LIMIT BY` 子句支持位置参数。要启用此功能，请开启 [enable&#95;positional&#95;arguments](/zh/operations/settings/settings#enable_positional_arguments) 设置。这样，例如，`ORDER BY 1,2` 会先按表中的第一列、再按第二列对行进行排序。

<div id="implementation-details">
  ## 实现细节
</div>

如果查询中省略了 `DISTINCT`、`GROUP BY` 和 `ORDER BY` 子句，以及 `IN` 和 `JOIN` 子查询，那么该查询将完全以流式方式处理，只需 O(1) 量级的 RAM。否则，如果未设置适当的限制，查询可能会消耗大量 RAM：

* `max_memory_usage`
* `max_rows_to_group_by`
* `max_rows_to_sort`
* `max_rows_in_distinct`
* `max_bytes_in_distinct`
* `max_rows_in_set`
* `max_bytes_in_set`
* `max_rows_in_join`
* `max_bytes_in_join`
* `max_bytes_before_external_sort`
* `max_bytes_ratio_before_external_sort`
* `max_bytes_before_external_group_by`
* `max_bytes_ratio_before_external_group_by`

更多信息，请参见“设置”部分。也可以使用外部排序 (将临时表保存到磁盘) 和外部聚合。

<div id="select-modifiers">
  ## SELECT 修饰符
</div>

你可以在 `SELECT` 查询中使用以下修饰符。

| 修饰符                                | 说明                                                                                                                                                           |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`APPLY`](./apply_modifier.md)     | 允许你对查询外层表表达式返回的每一行调用某个函数。                                                                                                                                    |
| [`EXCEPT`](./except_modifier.md)   | 指定从结果中排除一个或多个列名。所有匹配的列名都不会出现在输出结果中。                                                                                                                          |
| [`REPLACE`](./replace_modifier.md) | 指定一个或多个[表达式别名](/zh/sql-reference/syntax#expression-aliases)。每个别名都必须与 `SELECT *` 语句中的某个列名匹配。在输出列列表中，与该别名匹配的列会被替换为该 `REPLACE` 中指定的表达式。此修饰符不会更改列的名称或顺序，但可以更改值及其类型。 |

<div id="modifier-combinations">
  ### 修饰符组合
</div>

您可以分别使用各个修饰符，也可以将它们组合使用。

**示例：**

多次使用同一修饰符。

```sql
SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) APPLY(max) FROM columns_transformers;
```

```response
┌─max(length(toString(j)))─┬─max(length(toString(k)))─┐
│                        2 │                        3 │
└──────────────────────────┴──────────────────────────┘
```

在同一个查询中使用多个修饰符。

```sql
SELECT * REPLACE(i + 1 AS i) EXCEPT (j) APPLY(sum) from columns_transformers;
```

```response
┌─sum(plus(i, 1))─┬─sum(k)─┐
│             222 │    347 │
└─────────────────┴────────┘
```

<div id="settings-in-select-query">
  ## SELECT 查询中的 SETTINGS
</div>

你可以直接在 `SELECT` 查询中指定所需设置。该设置值仅对当前查询生效，并会在查询执行后恢复为默认值或之前的值。

如需了解其他设置方式，请参见[这里](/zh/operations/settings/overview)。

对于值为 true 的布尔设置，可以通过省略赋值来使用简写语法。仅指定设置名称时，系统会自动将其设为 `1` (true) 。

**示例**

```sql
SELECT * FROM some_table SETTINGS optimize_read_in_order=1, cast_keep_nullable=1;
```