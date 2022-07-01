---
sidebar_label: GROUP BY
---

# GROUP BY子句 {#select-group-by-clause}

`GROUP BY` 子句将 `SELECT` 查询结果转换为聚合模式，其工作原理如下:

-   `GROUP BY` 子句包含表达式列表（或单个表达式 -- 可以认为是长度为1的列表）。 这份名单充当 “grouping key”，而每个单独的表达式将被称为 “key expressions”.
-   在所有的表达式在 [SELECT](../../../sql-reference/statements/select/index.md), [HAVING](../../../sql-reference/statements/select/having)，和 [ORDER BY](../../../sql-reference/statements/select/order-by.md) 子句中 **必须** 基于键表达式进行计算 **或** 上 [聚合函数](../../../sql-reference/aggregate-functions/index.md) 在非键表达式（包括纯列）上。 换句话说，从表中选择的每个列必须用于键表达式或聚合函数内，但不能同时使用。
-   聚合结果 `SELECT` 查询将包含尽可能多的行，因为有唯一值 “grouping key” 在源表中。 通常这会显着减少行数，通常是数量级，但不一定：如果所有行数保持不变 “grouping key” 值是不同的。

!!! note "注"
    还有一种额外的方法可以在表上运行聚合。 如果查询仅在聚合函数中包含表列，则 `GROUP BY` 可以省略，并且通过一个空的键集合来假定聚合。 这样的查询总是只返回一行。

## 空处理 {#null-processing}

对于分组，ClickHouse解释 [NULL](../../../sql-reference/syntax.md#null-literal) 作为一个值，并且 `NULL==NULL`. 它不同于 `NULL` 在大多数其他上下文中的处理方式。

这里有一个例子来说明这意味着什么。

假设你有一张表:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

查询 `SELECT sum(x), y FROM t_null_big GROUP BY y` 结果:

``` text
┌─sum(x)─┬────y─┐
│      4 │    2 │
│      3 │    3 │
│      5 │ ᴺᵁᴸᴸ │
└────────┴──────┘
```

你可以看到 `GROUP BY` 为 `y = NULL` 总结 `x`，仿佛 `NULL` 是这个值。

如果你通过几个键 `GROUP BY`，结果会给你选择的所有组合，就好像 `NULL` 是一个特定的值。

## WITH TOTAL 修饰符 {#with-totals-modifier}

如果 `WITH TOTALS` 被指定，将计算另一行。 此行将具有包含默认值（零或空行）的关键列，以及包含跨所有行计算值的聚合函数列（ “total” 值）。

这个额外的行仅产生于 `JSON*`, `TabSeparated*`，和 `Pretty*` 格式，与其他行分开:

-   在 `JSON*` 格式，这一行是作为一个单独的输出 ‘totals’ 字段。
-   在 `TabSeparated*` 格式，该行位于主结果之后，前面有一个空行（在其他数据之后）。
-   在 `Pretty*` 格式时，该行在主结果之后作为单独的表输出。
-   在其他格式中，它不可用。

`WITH TOTALS` 可以以不同的方式运行时 [HAVING](../../../sql-reference/statements/select/having) 是存在的。 该行为取决于 `totals_mode` 设置。

### 配置总和处理 {#configuring-totals-processing}

默认情况下, `totals_mode = 'before_having'`. 在这种情况下, ‘totals’ 是跨所有行计算，包括那些不通过具有和 `max_rows_to_group_by`.

其他替代方案仅包括通过具有在 ‘totals’，并与设置不同的行为 `max_rows_to_group_by` 和 `group_by_overflow_mode = 'any'`.

`after_having_exclusive` – Don't include rows that didn't pass through `max_rows_to_group_by`. 换句话说, ‘totals’ 将有少于或相同数量的行，因为它会 `max_rows_to_group_by` 被省略。

`after_having_inclusive` – Include all the rows that didn't pass through ‘max_rows_to_group_by’ 在 ‘totals’. 换句话说, ‘totals’ 将有多个或相同数量的行，因为它会 `max_rows_to_group_by` 被省略。

`after_having_auto` – Count the number of rows that passed through HAVING. If it is more than a certain amount (by default, 50%), include all the rows that didn't pass through ‘max_rows_to_group_by’ 在 ‘totals’. 否则，不包括它们。

`totals_auto_threshold` – By default, 0.5. The coefficient for `after_having_auto`.

如果 `max_rows_to_group_by` 和 `group_by_overflow_mode = 'any'` 不使用，所有的变化 `after_having` 是相同的，你可以使用它们中的任何一个（例如, `after_having_auto`).

您可以使用 `WITH TOTALS` 在子查询中，包括在子查询 [JOIN](../../../sql-reference/statements/select/join.md) 子句（在这种情况下，将各自的总值合并）。

## 例子 {#examples}

示例:

``` sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

但是，与标准SQL相比，如果表没有任何行（根本没有任何行，或者使用 WHERE 过滤之后没有任何行），则返回一个空结果，而不是来自包含聚合函数初始值的行。

相对于MySQL（并且符合标准SQL），您无法获取不在键或聚合函数（常量表达式除外）中的某些列的某些值。 要解决此问题，您可以使用 ‘any’ 聚合函数（获取第一个遇到的值）或 ‘min/max’.

示例:

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- getting the first occurred page header for each domain.
FROM hits
GROUP BY domain
```

对于遇到的每个不同的键值, `GROUP BY` 计算一组聚合函数值。

`GROUP BY` 不支持数组列。

不能将常量指定为聚合函数的参数。 示例: `sum(1)`. 相反，你可以摆脱常数。 示例: `count()`.

## 实现细节 {#implementation-details}

聚合是面向列的 DBMS 最重要的功能之一，因此它的实现是ClickHouse中最优化的部分之一。 默认情况下，聚合使用哈希表在内存中完成。 它有 40+ 的特殊化自动选择取决于 “grouping key” 数据类型。

### 在外部存储器中分组 {#select-group-by-in-external-memory}

您可以启用将临时数据转储到磁盘以限制内存使用期间 `GROUP BY`.
该 [max_bytes_before_external_group_by](../../../operations/settings/query-complexity.md#settings-max_bytes_before_external_group_by) 设置确定倾销的阈值RAM消耗 `GROUP BY` 临时数据到文件系统。 如果设置为0（默认值），它将被禁用。

使用时 `max_bytes_before_external_group_by`，我们建议您设置 `max_memory_usage` 大约两倍高。 这是必要的，因为聚合有两个阶段：读取数据和形成中间数据（1）和合并中间数据（2）。 将数据转储到文件系统只能在阶段1中发生。 如果未转储临时数据，则阶段2可能需要与阶段1相同的内存量。

例如，如果 [max_memory_usage](../../../operations/settings/query-complexity.md#settings_max_memory_usage) 设置为10000000000，你想使用外部聚合，这是有意义的设置 `max_bytes_before_external_group_by` 到10000000000，和 `max_memory_usage` 到20000000000。 当触发外部聚合（如果至少有一个临时数据转储）时，RAM的最大消耗仅略高于 `max_bytes_before_external_group_by`.

通过分布式查询处理，在远程服务器上执行外部聚合。 为了使请求者服务器只使用少量的RAM，设置 `distributed_aggregation_memory_efficient` 到1。

当合并数据刷新到磁盘时，以及当合并来自远程服务器的结果时， `distributed_aggregation_memory_efficient` 设置被启用，消耗高达 `1/256 * the_number_of_threads` 从RAM的总量。

当启用外部聚合时，如果数据量小于 `max_bytes_before_external_group_by` (例如数据没有被 flushed), 查询执行速度和不在外部聚合的速度一样快. 如果临时数据被flushed到外部存储, 执行的速度会慢几倍 (大概是三倍).

如果你有一个 [ORDER BY](../../../sql-reference/statements/select/order-by.md) 用一个 [LIMIT](../../../sql-reference/statements/select/limit.md) 后 `GROUP BY`，然后使用的RAM的量取决于数据的量 `LIMIT`，不是在整个表。 但如果 `ORDER BY` 没有 `LIMIT`，不要忘记启用外部排序 (`max_bytes_before_external_sort`).
