---
toc_priority: 106
---

# argMax {#agg-function-argmax}

计算 `val` 最大值对应的 `arg` 值。 如果 `val` 最大值存在几个不同的 `arg` 值，输出遇到的第一个值。

这个函数的Tuple版本将返回 `val` 最大值对应的元组。本函数适合和 `SimpleAggregateFunction` 搭配使用。

**语法**

``` sql
argMax(arg, val)
```

或

``` sql
argMax(tuple(arg, val))
```

**参数**

-   `arg` — Argument.
-   `val` — Value.

**返回值**

-   `val` 最大值对应的 `arg` 值。

类型: 匹配 `arg` 类型。

对于输入中的元组:

-   元组 `(arg, val)`, 其中 `val` 最大值，`arg` 是对应的值。

类型: [元组](../../../sql-reference/data-types/tuple.md)。

**示例**

输入表:

``` text
┌─user─────┬─salary─┐
│ director │   5000 │
│ manager  │   3000 │
│ worker   │   1000 │
└──────────┴────────┘
```

查询:

``` sql
SELECT argMax(user, salary), argMax(tuple(user, salary), salary), argMax(tuple(user, salary)) FROM salary;
```

结果:

``` text
┌─argMax(user, salary)─┬─argMax(tuple(user, salary), salary)─┬─argMax(tuple(user, salary))─┐
│ director             │ ('director',5000)                   │ ('director',5000)           │
└──────────────────────┴─────────────────────────────────────┴─────────────────────────────┘
```
