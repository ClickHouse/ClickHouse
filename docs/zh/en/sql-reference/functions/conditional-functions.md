---
description: '条件函数文档'
sidebar_label: '条件'
slug: /sql-reference/functions/conditional-functions
title: '条件函数'
doc_type: 'reference'
---

<div id="overview">
  ## 概览
</div>

<div id="using-conditional-results-directly">
  ### 直接使用条件表达式的结果
</div>

条件表达式的结果始终为 `0`、`1` 或 `NULL`，因此你可以像下面这样直接使用这些结果：

```sql
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

<div id="null-values-in-conditionals">
  ### 条件表达式中的 NULL 值
</div>

当条件表达式涉及 `NULL` 值时，结果也会是 `NULL`。

```sql
SELECT
    NULL < 1,
    2 < NULL,
    NULL < NULL,
    NULL = NULL

┌─less(NULL, 1)─┬─less(2, NULL)─┬─less(NULL, NULL)─┬─equals(NULL, NULL)─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ             │ ᴺᵁᴸᴸ               │
└───────────────┴───────────────┴──────────────────┴────────────────────┘
```

因此，如果类型为 `Nullable`，你在构造查询时应格外小心。

下面的示例说明了这一点：由于没有给 `multiIf` 添加 equals 条件，结果会失败。

```sql
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

<div id="case-statement">
  ### CASE 语句
</div>

ClickHouse 中的 CASE 表达式提供了类似 SQL CASE 运算符的条件逻辑。它会对条件进行求值，并根据第一个匹配的条件返回相应的值。

ClickHouse 支持两种 CASE 形式：

1. `CASE WHEN ... THEN ... ELSE ... END`
   <br />
   这种形式具有完全的灵活性，内部使用 [multiIf](/zh/sql-reference/functions/conditional-functions#multiIf) 函数实现。每个条件都会独立求值，表达式中也可以包含非常量值。

```sql
SELECT
    number,
    CASE
        WHEN number % 2 = 0 THEN number + 1
        WHEN number % 2 = 1 THEN number * 10
        ELSE number
    END AS result
FROM system.numbers
WHERE number < 5;

-- is translated to
SELECT
    number,
    multiIf((number % 2) = 0, number + 1, (number % 2) = 1, number * 10, number) AS result
FROM system.numbers
WHERE number < 5

┌─number─┬─result─┐
│      0 │      1 │
│      1 │     10 │
│      2 │      3 │
│      3 │     30 │
│      4 │      5 │
└────────┴────────┘

5 rows in set. Elapsed: 0.002 sec.
```

2. `CASE <expr> WHEN <val1> THEN ... WHEN <val2> THEN ... ELSE ... END`
   <br />
   这种更紧凑的形式针对常量值匹配做了优化，内部使用 `caseWithExpression()`。

例如，下面的写法是有效的：

```sql
SELECT
    number,
    CASE number
        WHEN 0 THEN 100
        WHEN 1 THEN 200
        ELSE 0
    END AS result
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    caseWithExpression(number, 0, 100, 1, 200, 0) AS result
FROM system.numbers
WHERE number < 3

┌─number─┬─result─┐
│      0 │    100 │
│      1 │    200 │
│      2 │      0 │
└────────┴────────┘

3 rows in set. Elapsed: 0.002 sec.
```

这种写法也不要求返回表达式必须是常量。

```sql
SELECT
    number,
    CASE number
        WHEN 0 THEN number + 1
        WHEN 1 THEN number * 10
        ELSE number
    END
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    caseWithExpression(number, 0, number + 1, 1, number * 10, number)
FROM system.numbers
WHERE number < 3

┌─number─┬─caseWithExpr⋯0), number)─┐
│      0 │                        1 │
│      1 │                       10 │
│      2 │                        2 │
└────────┴──────────────────────────┘

3 rows in set. Elapsed: 0.001 sec.
```

<div id="caveats">
  #### 注意事项
</div>

ClickHouse 会在计算任何条件之前，先确定 CASE 表达式 (或其内部等价形式，例如 `multiIf`) 的结果类型。当返回表达式的类型不同时，例如使用了不同的时区或不同的数值类型时，这一点尤其重要。

* 结果类型会根据所有分支中的最大兼容类型来确定。
* 一旦选定该类型，所有其他分支都会被隐式转换为该类型——即使这些分支的逻辑在运行时根本不会执行。
* 对于 DateTime64 这类类型，由于时区是类型签名的一部分，可能会导致一些出人意料的行为：首次遇到的时区可能会被应用到所有分支，即使其他分支指定了不同的时区。

例如，下面所有行都会返回第一个匹配分支时区中的时间戳，即 `Asia/Kolkata`

```sql
SELECT
    number,
    CASE
        WHEN number = 0 THEN fromUnixTimestamp64Milli(0, 'Asia/Kolkata')
        WHEN number = 1 THEN fromUnixTimestamp64Milli(0, 'America/Los_Angeles')
        ELSE fromUnixTimestamp64Milli(0, 'UTC')
    END AS tz
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    multiIf(number = 0, fromUnixTimestamp64Milli(0, 'Asia/Kolkata'), number = 1, fromUnixTimestamp64Milli(0, 'America/Los_Angeles'), fromUnixTimestamp64Milli(0, 'UTC')) AS tz
FROM system.numbers
WHERE number < 3

┌─number─┬──────────────────────tz─┐
│      0 │ 1970-01-01 05:30:00.000 │
│      1 │ 1970-01-01 05:30:00.000 │
│      2 │ 1970-01-01 05:30:00.000 │
└────────┴─────────────────────────┘

3 rows in set. Elapsed: 0.011 sec.
```

这里，ClickHouse 会识别到多个 `DateTime64(3, <timezone>)` 返回类型。它会将首先看到的 `DateTime64(3, 'Asia/Kolkata'` 推断为公共类型，并将其他分支隐式转换为该类型。

这个问题可以通过转换为字符串来解决，以保留预期的时区格式：

```sql
SELECT
    number,
    multiIf(
        number = 0, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'Asia/Kolkata'),
        number = 1, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'America/Los_Angeles'),
        formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'UTC')
    ) AS tz
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    multiIf(number = 0, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'Asia/Kolkata'), number = 1, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'America/Los_Angeles'), formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'UTC')) AS tz
FROM system.numbers
WHERE number < 3

┌─number─┬─tz──────────────────┐
│      0 │ 1970-01-01 05:30:00 │
│      1 │ 1969-12-31 16:00:00 │
│      2 │ 1970-01-01 00:00:00 │
└────────┴─────────────────────┘

3 rows in set. Elapsed: 0.002 sec.
```

{/* 
  下面这些标签中的内容会在文档框架构建期间被替换为
  根据 system.functions 生成的文档。请不要修改或删除这些标签。
  参见：https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }