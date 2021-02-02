---
toc_priority: 106
---

# argMax {#agg-function-argmax}

语法: `argMax(arg, val)` 或 `argMax(tuple(arg, val))`

计算 `val` 最大值对应的 `arg`  值。 如果 `val` 最大值存在几个不同的 `arg` 值，输出遇到的第一个(`arg`)值。


这个函数的Tuple版本将返回`val`最大值对应的tuple。本函数适合和`SimpleAggregateFunction`搭配使用。

**示例:**

``` text
┌─user─────┬─salary─┐
│ director │   5000 │
│ manager  │   3000 │
│ worker   │   1000 │
└──────────┴────────┘
```

``` sql
SELECT argMax(user, salary), argMax(tuple(user, salary)) FROM salary
```

``` text
┌─argMax(user, salary)─┬─argMax(tuple(user, salary))─┐
│ director             │ ('director',5000)           │
└──────────────────────┴─────────────────────────────┘
```
