# 条件函数 {#tiao-jian-han-shu}

## 如果（cond，那么，否则），cond？ 运算符然后：else {#ifcond-then-else-cond-operator-then-else}

如果`cond ！= 0`则返回`then`，如果`cond = 0`则返回`else`。
`cond`必须是`UInt8`类型，`then`和`else`必须存在最低的共同类型。

`then`和`else`可以是`NULL`

## 多 {#multiif}

允许您在查询中更紧凑地编写[CASE](../operators/index.md#operator_case)运算符。

    multiIf(cond_1, then_1, cond_2, then_2...else)

**参数:**

-   `cond_N` — 函数返回`then_N`的条件。
-   `then_N` — 执行时函数的结果。
-   `else` — 如果没有满足任何条件，则为函数的结果。

该函数接受`2N + 1`参数。

**返回值**

该函数返回值«then\_N»或«else»之一，具体取决于条件`cond_N`。

**示例**

存在如下一张表

    ┌─x─┬────y─┐
    │ 1 │ ᴺᵁᴸᴸ │
    │ 2 │    3 │
    └───┴──────┘

执行查询 `SELECT multiIf(isNull(y) x, y < 3, y, NULL) FROM t_null`。结果：

    ┌─multiIf(isNull(y), x, less(y, 3), y, NULL)─┐
    │                                          1 │
    │                                       ᴺᵁᴸᴸ │
    └────────────────────────────────────────────┘

[来源文章](https://clickhouse.tech/docs/en/query_language/functions/conditional_functions/) <!--hide-->
