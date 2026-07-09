---
description: '选择最后遇到的值，与 `anyLast` 类似，但也可接受
  NULL。'
slug: /sql-reference/aggregate-functions/reference/last_value
title: 'last_value'
doc_type: 'reference'
---

选择最后遇到的值，与 `anyLast` 类似，但也可接受 NULL。
它通常应与 [窗口函数](../../window-functions/index.md) 一起使用。
如果不使用窗口函数，而源数据流又不是有序的，则结果将是随机的。

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

INSERT INTO test_data (a, b) VALUES (1,null), (2,3), (4, 5), (6,null)
```

<div id="example1">
  ### 示例 1
</div>

默认情况下，NULL 值会被忽略。

```sql
SELECT last_value(b) FROM test_data
```

```text
┌─last_value_ignore_nulls(b)─┐
│                          5 │
└────────────────────────────┘
```

<div id="example2">
  ### 示例 2
</div>

NULL 值会被忽略。

```sql
SELECT last_value(b) ignore nulls FROM test_data
```

```text
┌─last_value_ignore_nulls(b)─┐
│                          5 │
└────────────────────────────┘
```

<div id="example3">
  ### 示例 3
</div>

支持 NULL 值。

```sql
SELECT last_value(b) respect nulls FROM test_data
```

```text
┌─last_value_respect_nulls(b)─┐
│                        ᴺᵁᴸᴸ │
└─────────────────────────────┘
```

<div id="example4">
  ### 示例 4
</div>

使用带有 `ORDER BY` 的子查询来获得稳定的结果。

```sql
SELECT
    last_value_respect_nulls(b),
    last_value(b)
FROM
(
    SELECT *
    FROM test_data
    ORDER BY a ASC
)
```

```text
┌─last_value_respect_nulls(b)─┬─last_value(b)─┐
│                        ᴺᵁᴸᴸ │             5 │
└─────────────────────────────┴───────────────┘
```