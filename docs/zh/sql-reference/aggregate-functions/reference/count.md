---
sidebar_position: 1
---

# count {#agg_function-count}


计数行数或非空值。

ClickHouse支持以下 `count` 语法:
- `count(expr)` 或 `COUNT(DISTINCT expr)`。
- `count()` 或 `COUNT(*)`. 该 `count()` 语法是ClickHouse特定的。

**参数**

该函数可以采取:

-   零参数。
-   一个 [表达式](../../../sql-reference/syntax.md#syntax-expressions)。

**返回值**

-   如果没有参数调用函数，它会计算行数。
-   如果 [表达式](../../../sql-reference/syntax.md#syntax-expressions) 被传递，则该函数计数此表达式返回非null的次数。 如果表达式返回 [可为空](../../../sql-reference/data-types/nullable.md)类型的值，`count`的结果仍然不 `Nullable`。 如果表达式对于所有的行都返回 `NULL` ，则该函数返回 0 。

在这两种情况下，返回值的类型为 [UInt64](../../../sql-reference/data-types/int-uint.md)。

**详细信息**

ClickHouse支持 `COUNT(DISTINCT ...)` 语法，这种结构的行为取决于 [count_distinct_implementation](../../../operations/settings/settings.md#settings-count_distinct_implementation) 设置。 它定义了用于执行该操作的 [uniq\*](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)函数。 默认值是 [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)函数。

`SELECT count() FROM table` 这个查询未被优化，因为表中的条目数没有单独存储。 它从表中选择一个小列并计算其值的个数。

**示例**

示例1:

``` sql
SELECT count() FROM t
```

``` text
┌─count()─┐
│       5 │
└─────────┘
```

示例2:

``` sql
SELECT name, value FROM system.settings WHERE name = 'count_distinct_implementation'
```

``` text
┌─name──────────────────────────┬─value─────┐
│ count_distinct_implementation │ uniqExact │
└───────────────────────────────┴───────────┘
```

``` sql
SELECT count(DISTINCT num) FROM t
```

``` text
┌─uniqExact(num)─┐
│              3 │
└────────────────┘
```

这个例子表明 `count(DISTINCT num)` 是通过 `count_distinct_implementation` 的设定值 `uniqExact` 函数来执行的。
