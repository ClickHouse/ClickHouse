---
slug: /zh/sql-reference/aggregate-functions/reference/argmax
sidebar_position: 106
---

# argMax {#agg-function-argmax}

计算 `val` 最大值对应的 `arg` 值。

**语法**

``` sql
argMax(arg, val)
```

**参数**

-   `arg` — Argument.
-   `val` — Value.

**返回值**

-   `val` 最大值对应的 `arg` 值。

类型: 匹配 `arg` 类型。

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
SELECT argMax(user, salary), argMax(tuple(user, salary), salary) FROM salary;
```

结果:

``` text
┌─argMax(user, salary)─┬─argMax(tuple(user, salary), salary)─┐
│ director             │ ('director',5000)                   │
└──────────────────────┴─────────────────────────────────────┘
```
