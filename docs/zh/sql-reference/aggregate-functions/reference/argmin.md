---
slug: /zh/sql-reference/aggregate-functions/reference/argmin
sidebar_position: 105
---

# argMin {#agg-function-argmin}

语法: `argMin(arg, val)` 或 `argMin(tuple(arg, val))`

计算 `val` 最小值对应的 `arg` 值。

**示例:**

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
SELECT argMin(user, salary), argMin(tuple(user, salary), salary) FROM salary;
```

结果:

``` text
┌─argMin(user, salary)─┬─argMin(tuple(user, salary), salary)─┐
│ worker               │ ('worker',1000)                     │
└──────────────────────┴─────────────────────────────────────┘
```
