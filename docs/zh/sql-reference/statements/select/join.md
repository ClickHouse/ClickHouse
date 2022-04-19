---
toc_title: JOIN
---

# JOIN子句 {#select-join}

Join通过使用一个或多个表的公共值合并来自一个或多个表的列来生成新表。 它是支持SQL的数据库中的常见操作，它对应于 [关系代数](https://en.wikipedia.org/wiki/Relational_algebra#Joins_and_join-like_operators) 加入。 一个表连接的特殊情况通常被称为 “self-join”.

语法:

``` sql
SELECT <expr_list>
FROM <left_table>
[GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ASOF] JOIN <right_table>
(ON <expr_list>)|(USING <column_list>) ...
```

从表达式 `ON` 从子句和列 `USING` 子句被称为 “join keys”. 除非另有说明，加入产生一个 [笛卡尔积](https://en.wikipedia.org/wiki/Cartesian_product) 从具有匹配的行 “join keys”，这可能会产生比源表更多的行的结果。

## 支持的联接类型 {#select-join-types}

所有标准 [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) 支持类型:

-   `INNER JOIN`，只返回匹配的行。
-   `LEFT OUTER JOIN`，除了匹配的行之外，还返回左表中的非匹配行。
-   `RIGHT OUTER JOIN`，除了匹配的行之外，还返回右表中的非匹配行。
-   `FULL OUTER JOIN`，除了匹配的行之外，还会返回两个表中的非匹配行。
-   `CROSS JOIN`，产生整个表的笛卡尔积, “join keys” 是 **不** 指定。

`JOIN` 没有指定类型暗指 `INNER`. 关键字 `OUTER` 可以安全地省略。 替代语法 `CROSS JOIN` 在指定多个表 [FROM](../../../sql-reference/statements/select/from.md) 用逗号分隔。

ClickHouse中提供的其他联接类型:

-   `LEFT SEMI JOIN` 和 `RIGHT SEMI JOIN`,白名单 “join keys”，而不产生笛卡尔积。
-   `LEFT ANTI JOIN` 和 `RIGHT ANTI JOIN`，黑名单 “join keys”，而不产生笛卡尔积。
-   `LEFT ANY JOIN`, `RIGHT ANY JOIN` and `INNER ANY JOIN`, partially (for opposite side of `LEFT` and `RIGHT`) or completely (for `INNER` and `FULL`) disables the cartesian product for standard `JOIN` types.
-   `ASOF JOIN` and `LEFT ASOF JOIN`, joining sequences with a non-exact match. `ASOF JOIN` usage is described below.

## 严格 {#join-settings}

!!! note "注"
    可以使用以下方式复盖默认的严格性值 [join_default_strictness](../../../operations/settings/settings.md#settings-join_default_strictness) 设置。

    Also the behavior of ClickHouse server for `ANY JOIN` operations depends on the [any_join_distinct_right_table_keys](../../../operations/settings/settings.md#any_join_distinct_right_table_keys) setting.

### ASOF JOIN使用 {#asof-join-usage}

`ASOF JOIN` 当您需要连接没有完全匹配的记录时非常有用。

该算法需要表中的特殊列。 该列需要满足:

-   必须包含有序序列。
-   可以是以下类型之一: [Int*，UInt*](../../../sql-reference/data-types/int-uint.md), [Float\*](../../../sql-reference/data-types/float.md), [Date](../../../sql-reference/data-types/date.md), [DateTime](../../../sql-reference/data-types/datetime.md), [Decimal\*](../../../sql-reference/data-types/decimal.md).
-   不能是`JOIN`子句中唯一的列

语法 `ASOF JOIN ... ON`:

``` sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

您可以使用任意数量的相等条件和一个且只有一个最接近的匹配条件。 例如, `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

支持最接近匹配的运算符: `>`, `>=`, `<`, `<=`.

语法 `ASOF JOIN ... USING`:

``` sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`table_1.asof_column >= table_2.asof_column` 中， `ASOF JOIN` 使用 `equi_columnX` 来进行条件匹配， `asof_column` 用于JOIN最接近匹配。  `asof_column` 列总是在最后一个 `USING` 条件中。

例如，参考下表:

         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|----------   ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...

`ASOF JOIN`会从 `table_2` 中的用户事件时间戳找出和 `table_1` 中用户事件时间戳中最近的一个时间戳，来满足最接近匹配的条件。如果有得话，则相等的时间戳值是最接近的值。在此例中，`user_id` 列可用于条件匹配，`ev_time` 列可用于最接近匹配。在此例中，`event_1_1` 可以 JOIN `event_2_1`，`event_1_2` 可以JOIN `event_2_3`，但是 `event_2_2` 不能被JOIN。

!!! note "注"
    `ASOF JOIN`在 [JOIN](../../../engines/table-engines/special/join.md) 表引擎中 **不受** 支持。

## 分布式联接 {#global-join}

有两种方法可以执行涉及分布式表的join:

-   当使用正常 `JOIN`，将查询发送到远程服务器。 为了创建正确的表，在每个子查询上运行子查询，并使用此表执行联接。 换句话说，在每个服务器上单独形成右表。
-   使用时 `GLOBAL ... JOIN`，首先请求者服务器运行一个子查询来计算正确的表。 此临时表将传递到每个远程服务器，并使用传输的临时数据对其运行查询。

使用时要小心 `GLOBAL`. 有关详细信息，请参阅 [分布式子查询](../../../sql-reference/operators/in.md#select-distributed-subqueries) 科。

## 使用建议 {#usage-recommendations}

### 处理空单元格或空单元格 {#processing-of-empty-or-null-cells}

在连接表时，可能会出现空单元格。 设置 [join_use_nulls](../../../operations/settings/settings.md#join_use_nulls) 定义ClickHouse如何填充这些单元格。

如果 `JOIN` 键是 [可为空](../../../sql-reference/data-types/nullable.md) 字段，其中至少有一个键具有值的行 [NULL](../../../sql-reference/syntax.md#null-literal) 没有加入。

### 语法 {#syntax}

在指定的列 `USING` 两个子查询中必须具有相同的名称，并且其他列必须以不同的方式命名。 您可以使用别名更改子查询中的列名。

该 `USING` 子句指定一个或多个要联接的列，这将建立这些列的相等性。 列的列表设置不带括号。 不支持更复杂的连接条件。

### 语法限制 {#syntax-limitations}

对于多个 `JOIN` 单个子句 `SELECT` 查询:

-   通过以所有列 `*` 仅在联接表时才可用，而不是子查询。
-   该 `PREWHERE` 条款不可用。

为 `ON`, `WHERE`，和 `GROUP BY` 条款:

-   任意表达式不能用于 `ON`, `WHERE`，和 `GROUP BY` 子句，但你可以定义一个表达式 `SELECT` 子句，然后通过别名在这些子句中使用它。

### 性能 {#performance}

当运行 `JOIN`，与查询的其他阶段相关的执行顺序没有优化。 连接（在右表中搜索）在过滤之前运行 `WHERE` 和聚集之前。

每次使用相同的查询运行 `JOIN`，子查询再次运行，因为结果未缓存。 为了避免这种情况，使用特殊的 [加入我们](../../../engines/table-engines/special/join.md) 表引擎，它是一个用于连接的准备好的数组，总是在RAM中。

在某些情况下，使用效率更高 [IN](../../../sql-reference/operators/in.md) 而不是 `JOIN`.

如果你需要一个 `JOIN` 对于连接维度表（这些是包含维度属性的相对较小的表，例如广告活动的名称）， `JOIN` 由于每个查询都会重新访问正确的表，因此可能不太方便。 对于这种情况下，有一个 “external dictionaries” 您应该使用的功能 `JOIN`. 有关详细信息，请参阅 [外部字典](../../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) 科。

### 内存限制 {#memory-limitations}

默认情况下，ClickHouse使用 [哈希联接](https://en.wikipedia.org/wiki/Hash_join) 算法。 ClickHouse采取 `<right_table>` 并在RAM中为其创建哈希表。 在某个内存消耗阈值之后，ClickHouse回退到合并联接算法。

如果需要限制联接操作内存消耗，请使用以下设置:

-   [max_rows_in_join](../../../operations/settings/query-complexity.md#settings-max_rows_in_join) — Limits number of rows in the hash table.
-   [max_bytes_in_join](../../../operations/settings/query-complexity.md#settings-max_bytes_in_join) — Limits size of the hash table.

当任何这些限制达到，ClickHouse作为 [join_overflow_mode](../../../operations/settings/query-complexity.md#settings-join_overflow_mode) 设置指示。

## 例子 {#examples}

示例:

``` sql
SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY LEFT JOIN
(
    SELECT
        CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
) USING CounterID
ORDER BY hits DESC
LIMIT 10
```

``` text
┌─CounterID─┬───hits─┬─visits─┐
│   1143050 │ 523264 │  13665 │
│    731962 │ 475698 │ 102716 │
│    722545 │ 337212 │ 108187 │
│    722889 │ 252197 │  10547 │
│   2237260 │ 196036 │   9522 │
│  23057320 │ 147211 │   7689 │
│    722818 │  90109 │  17847 │
│     48221 │  85379 │   4652 │
│  19762435 │  77807 │   7026 │
│    722884 │  77492 │  11056 │
└───────────┴────────┴────────┘
```
