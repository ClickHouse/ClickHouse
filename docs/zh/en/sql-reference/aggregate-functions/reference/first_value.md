---
description: '它是 any 的别名，但为兼容 Window Functions 而引入，因为在某些情况下需要处理 `NULL` 值（默认情况下，所有 ClickHouse 聚合函数都会忽略 NULL 值）。'
slug: /sql-reference/aggregate-functions/reference/first_value
title: 'first_value'
doc_type: 'reference'
---

它是 [`any`](../../../sql-reference/aggregate-functions/reference/any.md) 的别名，但为兼容 [Window Functions](../../window-functions/index.md) 而引入，因为在某些情况下需要处理 `NULL` 值 (默认情况下，所有 ClickHouse 聚合函数都会忽略 NULL 值) 。

它支持声明一个用于保留 NULL 值的修饰符 (`RESPECT NULLS`) ，既可用于 [Window Functions](../../window-functions/index.md)，也可用于常规聚合。

与 `any` 一样，在不使用 Window Functions 时，如果源 stream 不是有序的，则结果将是随机的，并且返回类型
与输入类型一致 (仅当输入为 Nullable 或添加了 -OrNull 组合器时，才会返回 NULL) 。

<div id="examples">
  ## 示例
</div>

```sql
CREATE TABLE test_data
(
    a Int64,
    b Nullable(Int64)
)
ENGINE = Memory;

INSERT INTO test_data (a, b) VALUES (1,null), (2,3), (4, 5), (6,null);
```

<div id="example1">
  ### 示例 1
</div>

默认情况下，会忽略 NULL 值。

```sql
SELECT first_value(b) FROM test_data;
```

```text
┌─any(b)─┐
│      3 │
└────────┘
```

<div id="example2">
  ### 示例 2
</div>

NULL 值会被忽略。

```sql
SELECT first_value(b) ignore nulls FROM test_data
```

```text
┌─any(b) IGNORE NULLS ─┐
│                    3 │
└──────────────────────┘
```

<div id="example3">
  ### 示例 3
</div>

支持 NULL 值。

```sql
SELECT first_value(b) respect nulls FROM test_data
```

```text
┌─any(b) RESPECT NULLS ─┐
│                  ᴺᵁᴸᴸ │
└───────────────────────┘
```

<div id="example4">
  ### 示例 4
</div>

使用带有 `ORDER BY` 的子查询来获得稳定的结果。

```sql
SELECT
    first_value_respect_nulls(b),
    first_value(b)
FROM
(
    SELECT *
    FROM test_data
    ORDER BY a ASC
)
```

```text
┌─any_respect_nulls(b)─┬─any(b)─┐
│                 ᴺᵁᴸᴸ │      3 │
└──────────────────────┴────────┘
```