---
description: 'GROUP BY 子句文档'
sidebar_label: 'GROUP BY'
slug: /sql-reference/statements/select/group-by
title: 'GROUP BY 子句'
doc_type: 'reference'
---

`GROUP BY` 子句会将 `SELECT` 查询切换为聚合模式，其工作方式如下：

* `GROUP BY` 子句包含一个 expressions 列表 (或单个 expression，此时视为长度为 1 的列表) 。该列表用作“分组键”，其中每个单独的 expression 称为“键表达式”。
* [SELECT](/zh/sql-reference/statements/select/index.md)、[HAVING](/zh/sql-reference/statements/select/having.md) 和 [ORDER BY](/zh/sql-reference/statements/select/order-by.md) 子句中的所有 expressions **必须** 基于 key expressions **或** 基于作用于非键 expressions (包括普通列) 的 [聚合函数](../../../sql-reference/aggregate-functions/index.md) 来计算。换句话说，从表中选出的每一列都必须要么用于 键表达式，要么位于 aggregate function 内，但不能同时用于两者。
* 聚合后的 `SELECT` 查询结果所包含的行数，将等于 source table 中“分组键”唯一值的数量。通常，这会显著减少行数，而且往往会减少几个数量级；但并非总是如此：如果所有“分组键”值都互不相同，行数将保持不变。

如果要按列编号而不是 column names 对表中的数据进行分组，请启用设置 [enable&#95;positional&#95;arguments](/zh/operations/settings/settings#enable_positional_arguments)。

:::note
还有一种对表执行聚合的方法。如果查询仅在 聚合函数 内使用表列，则可以省略 `GROUP BY 子句`，此时会假定按空键集进行聚合。这类查询始终只返回一行。
:::

<div id="null-processing">
  ## NULL 处理
</div>

在分组时，ClickHouse 将 [NULL](/zh/sql-reference/syntax#null) 视为一个值，并且 `NULL==NULL`。这与大多数其他场景中的 `NULL` 处理方式不同。

下面通过一个示例来说明这意味着什么。

假设你有如下这张表：

```text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

查询 `SELECT sum(x), y FROM t_null_big GROUP BY y` 的结果如下：

```text
┌─sum(x)─┬────y─┐
│      4 │    2 │
│      3 │    3 │
│      5 │ ᴺᵁᴸᴸ │
└────────┴──────┘
```

你可以看到，在 `GROUP BY` 中，对于 `y = NULL` 的情况，`x` 也被求和了，就好像 `NULL` 是一个实际的值。

如果向 `GROUP BY` 传入多个键，结果会给出所选项的所有组合，就好像 `NULL` 是一个特定的值。

<div id="rollup-modifier">
  ## ROLLUP 修饰符
</div>

`ROLLUP` 修饰符用于按照 `GROUP BY` 列表中键表达式的顺序计算小计。小计行会添加在结果表之后。

小计按逆序计算：首先为列表中的最后一个键表达式计算小计，然后是前一个，依此类推，直到第一个键表达式。

在小计行中，已经“分组”的键表达式的值会被设为 `0` 或空字符串。

:::note
请注意，[HAVING](/zh/sql-reference/statements/select/having.md) 子句可能会影响小计结果。
:::

**示例**

考虑表 t：

```text
┌─year─┬─month─┬─day─┐
│ 2019 │     1 │   5 │
│ 2019 │     1 │  15 │
│ 2020 │     1 │   5 │
│ 2020 │     1 │  15 │
│ 2020 │    10 │   5 │
│ 2020 │    10 │  15 │
└──────┴───────┴─────┘
```

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY ROLLUP(year, month, day);
```

由于 `GROUP BY` 部分包含三个键表达式，因此结果中有四个从右向左“逐级汇总”得到的表：

* `GROUP BY year, month, day`;
* `GROUP BY year, month` (`day` 列填充为零) ；
* `GROUP BY year` (此时 `month` 和 `day` 列都填充为零) ；
* 以及总计 (这三个键表达式列都为零) 。

```text title="Response"
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │    10 │  15 │       1 │
│ 2020 │     1 │   5 │       1 │
│ 2019 │     1 │   5 │       1 │
│ 2020 │     1 │  15 │       1 │
│ 2019 │     1 │  15 │       1 │
│ 2020 │    10 │   5 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     1 │   0 │       2 │
│ 2020 │     1 │   0 │       2 │
│ 2020 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     0 │   0 │       2 │
│ 2020 │     0 │   0 │       4 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   0 │       6 │
└──────┴───────┴─────┴─────────┘
```

同一个查询也可以使用 `WITH` 关键字来编写。

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH ROLLUP;
```

**另请参阅**

* 有关 SQL 标准兼容性，请参见 [group&#95;by&#95;use&#95;nulls](/zh/operations/settings/settings.md#group_by_use_nulls) 设置。

<div id="cube-modifier">
  ## CUBE 修饰符
</div>

`CUBE` 修饰符用于对 `GROUP BY` 列表中键表达式的每一种组合计算小计。小计行会附加在结果表之后。

在小计行中，所有“已分组”键表达式的值都会被设为 `0` 或空字符串。

:::note
请注意，[HAVING](/zh/sql-reference/statements/select/having.md) 子句可能会影响小计结果。
:::

**示例**

考虑表 t：

```text
┌─year─┬─month─┬─day─┐
│ 2019 │     1 │   5 │
│ 2019 │     1 │  15 │
│ 2020 │     1 │   5 │
│ 2020 │     1 │  15 │
│ 2020 │    10 │   5 │
│ 2020 │    10 │  15 │
└──────┴───────┴─────┘
```

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY CUBE(year, month, day);
```

由于 `GROUP BY` 部分包含三个键表达式，因此结果中会有八个表，分别对应所有键表达式组合的小计：

* `GROUP BY year, month, day`
* `GROUP BY year, month`
* `GROUP BY year, day`
* `GROUP BY year`
* `GROUP BY month, day`
* `GROUP BY month`
* `GROUP BY day`
* 以及总计。

未包含在 `GROUP BY` 中的列将用零填充。

```text title="Response"
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │    10 │  15 │       1 │
│ 2020 │     1 │   5 │       1 │
│ 2019 │     1 │   5 │       1 │
│ 2020 │     1 │  15 │       1 │
│ 2019 │     1 │  15 │       1 │
│ 2020 │    10 │   5 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     1 │   0 │       2 │
│ 2020 │     1 │   0 │       2 │
│ 2020 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │     0 │   5 │       2 │
│ 2019 │     0 │   5 │       1 │
│ 2020 │     0 │  15 │       2 │
│ 2019 │     0 │  15 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     0 │   0 │       2 │
│ 2020 │     0 │   0 │       4 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     1 │   5 │       2 │
│    0 │    10 │  15 │       1 │
│    0 │    10 │   5 │       1 │
│    0 │     1 │  15 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     1 │   0 │       4 │
│    0 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   5 │       3 │
│    0 │     0 │  15 │       3 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   0 │       6 │
└──────┴───────┴─────┴─────────┘
```

同样的查询也可以使用 `WITH` 关键字来编写。

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH CUBE;
```

**另请参阅**

* [group&#95;by&#95;use&#95;nulls](/zh/operations/settings/settings.md#group_by_use_nulls) 设置，用于兼容 SQL 标准。

<div id="with-totals-modifier">
  ## WITH TOTALS 修饰符
</div>

如果指定了 `WITH TOTALS` 修饰符，则会额外计算出一行。该行的键列包含默认值 (零或空字符串) ，而聚合函数列则包含基于所有行计算得到的值 (即“总计”值) 。

这个额外的行仅会在 `JSON*`、`TabSeparated*` 和 `Pretty*` 格式中生成，并与其他行分开输出：

* 在 `XML` 和 `JSON*` 格式中，这一行会作为单独的 `totals` 字段输出。
* 在 `TabSeparated*`、`CSV*` 和 `Vertical` 格式中，这一行会出现在主结果之后，并且前面有一个空行 (位于其他数据之后) 。
* 在 `Pretty*` 格式中，这一行会在主结果后作为单独的表输出。
* 在 `Template` 格式中，这一行会按照指定模板输出。
* 在其他格式中不可用。

:::note
totals 会在 `SELECT` 查询的结果中输出，而不会在 `INSERT INTO ... SELECT` 中输出。
:::

当存在 [HAVING](/zh/sql-reference/statements/select/having.md) 时，`WITH TOTALS` 的执行方式可以不同。其行为取决于 `totals_mode` 设置。

<div id="configuring-totals-processing">
  ### 配置 Totals 处理方式
</div>

默认情况下，`totals_mode = 'before_having'`。在这种情况下，&#39;totals&#39; 会基于所有行计算，包括未通过 HAVING 和 `max_rows_to_group_by` 的行。

其他选项则只会将通过 HAVING 的行计入 &#39;totals&#39;，并且在设置 `max_rows_to_group_by` 和 `group_by_overflow_mode = 'any'` 时会表现出不同的行为。

`after_having_exclusive` – 不包含未通过 `max_rows_to_group_by` 的行。换句话说，与省略 `max_rows_to_group_by` 时相比，&#39;totals&#39; 的行数会更少或相同。

`after_having_inclusive` – 将所有未通过 `max_rows_to_group_by` 的行都包含在 &#39;totals&#39; 中。换句话说，与省略 `max_rows_to_group_by` 时相比，&#39;totals&#39; 的行数会更多或相同。

`after_having_auto` – 统计通过 HAVING 的行数。如果超过某个阈值 (默认情况下为 50%) ，则将所有未通过 `max_rows_to_group_by` 的行都包含在 &#39;totals&#39; 中；否则不包含这些行。

`totals_auto_threshold` – 默认值为 0.5。它是 `after_having_auto` 的系数。

如果未使用 `max_rows_to_group_by` 和 `group_by_overflow_mode = 'any'`，那么 `after_having` 的所有变体都相同，你可以使用其中任意一种 (例如 `after_having_auto`) 。

你可以在子查询中使用 `WITH TOTALS`，包括 [JOIN](/zh/sql-reference/statements/select/join.md) 子句中的子查询 (在这种情况下，对应的总计值会被合并) 。

<div id="group-by-all">
  ## GROUP BY ALL
</div>

`GROUP BY ALL` 等同于列出所有已在 SELECT 中指定且不是聚合函数的表达式。

例如：

```sql
SELECT
    a * 2,
    b,
    count(c),
FROM t
GROUP BY ALL
```

与之相同

```sql
SELECT
    a * 2,
    b,
    count(c),
FROM t
GROUP BY a * 2, b
```

对于一种特殊情况：如果某个函数的参数同时包含聚合函数和其他字段，`GROUP BY` 键将包含我们能从中提取出的尽可能多的非聚合字段。

例如：

```sql
SELECT
    substring(a, 4, 2),
    substring(substring(a, 1, 2), 1, count(b))
FROM t
GROUP BY ALL
```

与……相同

```sql
SELECT
    substring(a, 4, 2),
    substring(substring(a, 1, 2), 1, count(b))
FROM t
GROUP BY substring(a, 4, 2), substring(a, 1, 2)
```

<div id="examples">
  ## 示例
</div>

示例：

```sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

与 MySQL 不同 (且符合标准 SQL) ，你不能获取某个既不在键中、也不在聚合函数中的列的值 (常量表达式除外) 。要解决这个问题，可以使用 `any` 聚合函数 (返回遇到的第一个值) 或 `min/max`。

示例：

```sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- getting the first occurred page header for each domain.
FROM hits
GROUP BY domain
```

对于每个遇到的不同键值，`GROUP BY` 都会计算出一组聚合函数值。

<div id="grouping-sets-modifier">
  ## `GROUPING SETS` 修饰符
</div>

这是最通用的一种修饰符。
该修饰符允许手动指定多个聚合键集合 (分组集) 。
系统会针对每个分组集分别执行聚合，然后将所有结果合并。
如果某一列未包含在某个分组集中，则会以默认值填充。

换句话说，上述修饰符都可以用 `GROUPING SETS` 表示。
尽管带有 `ROLLUP`、`CUBE` 和 `GROUPING SETS` 修饰符的查询在语法上等价，但它们的执行方式可能有所不同。
`GROUPING SETS` 会尽量将所有操作并行执行，而 `ROLLUP` 和 `CUBE` 则会在单个线程中完成聚合结果的最终合并。

当源列包含默认值时，可能很难判断某一行是否属于将这些列用作键的聚合结果。
要解决这个问题，必须使用 `GROUPING` 函数。

**示例**

以下两个查询是等价的。

```sql
-- Query 1
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH ROLLUP;

-- Query 2
SELECT year, month, day, count(*) FROM t GROUP BY
GROUPING SETS
(
    (year, month, day),
    (year, month),
    (year),
    ()
);
```

**另请参见**

* [group&#95;by&#95;use&#95;nulls](/zh/operations/settings/settings.md#group_by_use_nulls) 设置，用于实现 SQL 标准兼容性。

<div id="implementation-details">
  ## 实现细节
</div>

聚合是列式 DBMS 最重要的特性之一，因此其实现也是 ClickHouse 中优化程度最高的部分之一。默认情况下，聚合在内存中使用哈希表完成。它有 40 多种特化实现，会根据“分组键”数据类型自动选择。

<div id="group-by-optimization-depending-on-table-sorting-key">
  ### 根据表排序键进行的 GROUP BY 优化
</div>

如果表按某个键排序，并且 `GROUP BY` 表达式至少包含排序键的前缀或单射函数，就可以更高效地进行聚合。在这种情况下，当从表中读取到新的键时，聚合的中间结果就可以完成最终计算并发送给客户端。此行为可通过 [optimize&#95;aggregation&#95;in&#95;order](../../../operations/settings/settings.md#optimize_aggregation_in_order) 设置启用。这种优化可减少聚合期间的内存使用，但在某些情况下也可能会降低查询执行速度。

<div id="group-by-in-external-memory">
  ### 外部内存中的 GROUP BY
</div>

你可以启用将临时数据转储到磁盘，以限制 `GROUP BY` 期间的内存使用量。
[max&#95;bytes&#95;before&#95;external&#95;group&#95;by](/zh/operations/settings/settings#max_bytes_before_external_group_by) 设置用于确定将 `GROUP BY` 临时数据转储到文件系统时的 RAM 使用阈值。如果设为 0 (默认值) ，则表示禁用。
或者，你也可以设置 [max&#95;bytes&#95;ratio&#95;before&#95;external&#95;group&#95;by](/zh/operations/settings/settings#max_bytes_ratio_before_external_group_by)，这样只有当查询使用的内存达到某个阈值时，`GROUP BY` 才会使用外部内存。

使用 `max_bytes_before_external_group_by` 时，我们建议将 `max_memory_usage` 设置为大约其两倍 (或者设置 `max_bytes_ratio_before_external_group_by=0.5`) 。这是必要的，因为聚合分为两个阶段：读取数据并形成中间数据 (1)，以及合并中间数据 (2)。只有在第 1 阶段才能将数据转储到文件系统。如果临时数据没有被转储，那么第 2 阶段所需的内存最多可能与第 1 阶段相同。

例如，如果 [max&#95;memory&#95;usage](/zh/operations/settings/settings#max_memory_usage) 设置为 10000000000，并且你希望使用外部聚合，那么将 `max_bytes_before_external_group_by` 设置为 10000000000，同时将 `max_memory_usage` 设置为 20000000000 是合理的。当外部聚合被触发时 (前提是至少发生过一次临时数据转储) ，RAM 的最大消耗只会略高于 `max_bytes_before_external_group_by`。

在分布式查询处理中，外部聚合会在远程服务器上执行。为了让请求服务器只使用少量 RAM，请将 `distributed_aggregation_memory_efficient` 设置为 1。

在合并已写入磁盘的数据时，以及在启用 `distributed_aggregation_memory_efficient` 设置后合并来自远程服务器的结果时，最多会消耗总 RAM 量中的 `1/256 * the_number_of_threads`。

启用外部聚合后，如果数据量小于 `max_bytes_before_external_group_by` (即数据未写入磁盘) ，则查询运行速度与不使用外部聚合时一样快。如果有任何临时数据被写入磁盘，运行时间将延长数倍 (大约三倍) 。

如果你在 `GROUP BY` 之后使用带有 [LIMIT](/zh/sql-reference/statements/select/limit.md) 的 [ORDER BY](/zh/sql-reference/statements/select/order-by.md)，那么已使用的 RAM 量取决于 `LIMIT` 中的数据量，而不是整个表中的数据量。但如果 `ORDER BY` 没有 `LIMIT`，不要忘记启用外部排序 (`max_bytes_before_external_sort`) 。