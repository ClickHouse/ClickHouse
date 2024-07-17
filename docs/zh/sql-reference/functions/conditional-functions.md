---
slug: /zh/sql-reference/functions/conditional-functions
---
# 条件函数 {#tiao-jian-han-shu}

## if {#if}

控制条件分支。 与大多数系统不同，ClickHouse始终评估两个表达式 `then` 和 `else`。

**语法**

``` sql
SELECT if(cond, then, else)
```

如果条件 `cond` 的计算结果为非零值，则返回表达式 `then` 的结果，并且跳过表达式 `else` 的结果（如果存在）。 如果 `cond` 为零或 `NULL`，则将跳过 `then` 表达式的结果，并返回 `else` 表达式的结果（如果存在）。

您可以使用[short_circuit_function_evaluation](../../operations/settings/settings.md#short-circuit-function-evaluation) 设置，来根据短路方案计算 `if` 函数。如果启用此设置，则仅在`cond`为真的时，加载`then`表达式，此时不加载`else`表达式。仅在`cond`为假时，加载`else`表达式，此时不加载`then`表达式。例如，执行查询`SELECT if(number = 0, 0, intDiv(42, number)) FROM numbers(10)`时不会抛出除以零的异常，因为`intDiv(42, number)`会仅对不满足条件`number = 0`的数字进行处理。

**参数**

-   `cond` – 条件结果可以为零或不为零。 类型是 UInt8，Nullable(UInt8) 或 NULL。
-   `then` - 如果满足条件则返回的表达式。
-   `else` - 如果不满足条件则返回的表达式。

**返回值**

该函数执行 `then` 和 `else` 表达式并返回其结果，这取决于条件 `cond` 最终是否为零。

**示例**

查询:

``` sql
SELECT if(1, plus(2, 2), plus(2, 6))
```

结果:

``` text
┌─plus(2, 2)─┐
│          4 │
└────────────┘
```

查询:

``` sql
SELECT if(0, plus(2, 2), plus(2, 6))
```

结果:

``` text
┌─plus(2, 6)─┐
│          8 │
└────────────┘
```

-   `then` 和 `else` 必须具有最低的通用类型。

**示例:**

给定表`LEFT_RIGHT`:

``` sql
SELECT *
FROM LEFT_RIGHT

┌─left─┬─right─┐
│ ᴺᵁᴸᴸ │     4 │
│    1 │     3 │
│    2 │     2 │
│    3 │     1 │
│    4 │  ᴺᵁᴸᴸ │
└──────┴───────┘
```

下面的查询比较了 `left` 和 `right` 的值:

``` sql
SELECT
    left,
    right,
    if(left < right, 'left is smaller than right', 'right is smaller or equal than left') AS is_smaller
FROM LEFT_RIGHT
WHERE isNotNull(left) AND isNotNull(right)

┌─left─┬─right─┬─is_smaller──────────────────────────┐
│    1 │     3 │ left is smaller than right          │
│    2 │     2 │ right is smaller or equal than left │
│    3 │     1 │ right is smaller or equal than left │
└──────┴───────┴─────────────────────────────────────┘
```

注意：在此示例中未使用'NULL'值，请检查[条件中的NULL值](#null-values-in-conditionals) 部分。

## 三元运算符 {#ternary-operator}

与 `if` 函数相同。

语法: `cond ? then : else`

如果`cond ！= 0`则返回`then`，如果`cond = 0`则返回`else`。

-   `cond`必须是`UInt8`类型，`then`和`else`必须存在最低的共同类型。

-   `then`和`else`可以是`NULL`

**参考**

-   [ifNotFinite](../../sql-reference/functions/other-functions.md#ifnotfinite)。

## multiIf {#multiif}

允许您在查询中更紧凑地编写[CASE](../operators/index.md#operator_case)运算符。

**语法**

``` sql
multiIf(cond_1, then_1, cond_2, then_2, ..., else)
```

您可以使用[short_circuit_function_evaluation](../../operations/settings/settings.md#short-circuit-function-evaluation) 设置，根据短路方案计算 `multiIf` 函数。如果启用此设置，则 `then_i` 表达式仅在 `((NOT cond_1) AND (NOT cond_2) AND ... AND (NOT cond_{i-1}) AND cond_i)` 为真，`cond_i ` 将仅对 `((NOT cond_1) AND (NOT cond_2) AND ... AND (NOT cond_{i-1}))` 为真的行进行执行。例如，执行查询“SELECT multiIf(number = 2, intDiv(1, number), number = 5) FROM numbers(10)”时不会抛出除以零的异常。

**参数:**

-   `cond_N` — 函数返回`then_N`的条件。
-   `then_N` — 执行时函数的结果。
-   `else` — 如果没有满足任何条件，则为函数的结果。

该函数接受`2N + 1`参数。

**返回值**

该函数返回值«then_N»或«else»之一，具体取决于条件`cond_N`。

**示例**

再次使用表 `LEFT_RIGHT` 。

``` sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'left is greater', left = right, 'Both equal', 'Null value') AS result
FROM LEFT_RIGHT

┌─left─┬─right─┬─result──────────┐
│ ᴺᵁᴸᴸ │     4 │ Null value      │
│    1 │     3 │ left is smaller │
│    2 │     2 │ Both equal      │
│    3 │     1 │ left is greater │
│    4 │  ᴺᵁᴸᴸ │ Null value      │
└──────┴───────┴─────────────────┘
```
## 直接使用条件结果 {#using-conditional-results-directly}

条件结果始终为 `0`、 `1` 或 `NULL`。 因此，你可以像这样直接使用条件结果：

``` sql
SELECT left < right AS is_small
FROM LEFT_RIGHT

┌─is_small─┐
│     ᴺᵁᴸᴸ │
│        1 │
│        0 │
│        0 │
│     ᴺᵁᴸᴸ │
└──────────┘
```

## 条件中的NULL值 {#null-values-in-conditionals}

当条件中包含 `NULL` 值时，结果也将为 `NULL`。

``` sql
SELECT
    NULL < 1,
    2 < NULL,
    NULL < NULL,
    NULL = NULL

┌─less(NULL, 1)─┬─less(2, NULL)─┬─less(NULL, NULL)─┬─equals(NULL, NULL)─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ             │ ᴺᵁᴸᴸ               │
└───────────────┴───────────────┴──────────────────┴────────────────────┘
```

因此，如果类型是 `Nullable`，你应该仔细构造查询。

以下示例说明这一点。

``` sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'right is smaller', 'Both equal') AS faulty_result
FROM LEFT_RIGHT

┌─left─┬─right─┬─faulty_result────┐
│ ᴺᵁᴸᴸ │     4 │ Both equal       │
│    1 │     3 │ left is smaller  │
│    2 │     2 │ Both equal       │
│    3 │     1 │ right is smaller │
│    4 │  ᴺᵁᴸᴸ │ Both equal       │
└──────┴───────┴──────────────────┘
```
