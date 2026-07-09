---
description: 'ClickHouse 中 Tuple 数据类型的文档'
sidebar_label: 'Tuple(T1, T2, ...)'
sidebar_position: 34
slug: /sql-reference/data-types/tuple
title: 'Tuple(T1, T2, ...)'
doc_type: 'reference'
---

由一组元素组成的 Tuple，其中每个元素都有各自的[类型](/zh/sql-reference/data-types)。Tuple 至少必须包含一个元素。

Tuple 可用于临时列分组。在查询中使用 IN 表达式时，可以将多列分组；它也可用于指定 lambda 函数的某些形式参数。更多信息，请参见 [IN operators](../../sql-reference/operators/in.md) 和 [Higher order functions](/zh/sql-reference/functions/overview#higher-order-functions) 章节。

Tuple 也可以作为查询结果。在这种情况下，对于 JSON 以外的文本格式，各个值会在 `()` 中以逗号分隔；而在 JSON 格式中，Tuple 会作为数组输出 (使用 `[]`) 。

<div id="creating-tuples">
  ## 创建 Tuple
</div>

您可以使用函数来创建 Tuple：

```sql
tuple(T1, T2, ...)
```

创建 Tuple 的示例：

```sql
SELECT tuple(1, 'a') AS x, toTypeName(x)
```

```text
┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘
```

Tuple 可以只包含一个元素

示例：

```sql
SELECT tuple('a') AS x;
```

```text
┌─x─────┐
│ ('a') │
└───────┘
```

语法 `(tuple_element1, tuple_element2)` 可用于创建包含多个元素的Tuple，而无需调用 `tuple()` 函数。

示例：

```sql
SELECT (1, 'a') AS x, (today(), rand(), 'someString') AS y, ('a') AS not_a_tuple;
```

```text
┌─x───────┬─y──────────────────────────────────────┬─not_a_tuple─┐
│ (1,'a') │ ('2022-09-21',2006973416,'someString') │ a           │
└─────────┴────────────────────────────────────────┴─────────────┘
```

<div id="data-type-detection">
  ## 数据类型检测
</div>

在即时创建Tuple时，ClickHouse 会将Tuple参数的类型推断为能够容纳所提供参数值的最小类型。如果该值为 [NULL](/zh/operations/settings/formats#input_format_null_as_default)，则推断出的类型为 [Nullable](../../sql-reference/data-types/nullable.md)。

自动检测数据类型的示例：

```sql
SELECT tuple(1, NULL) AS x, toTypeName(x)
```

```text
┌─x─────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1, NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└───────────┴─────────────────────────────────┘
```

<div id="referring-to-tuple-elements">
  ## 引用 Tuple 的元素
</div>

Tuple 的元素可以通过名称或索引来引用：

```sql title="Query"
CREATE TABLE named_tuples (`a` Tuple(s String, i Int64)) ENGINE = Memory;
INSERT INTO named_tuples VALUES (('y', 10)), (('x',-10));

SELECT a.s FROM named_tuples; -- by name
SELECT a.2 FROM named_tuples; -- by index
```

```text title="Response"
┌─a.s─┐
│ y   │
│ x   │
└─────┘

┌─tupleElement(a, 2)─┐
│                 10 │
│                -10 │
└────────────────────┘
```

<div id="comparison-operations-with-tuple">
  ## Tuple 的比较操作
</div>

两个Tuple按从左到右的顺序依次比较各个元素。如果第一个Tuple中的某个元素大于 (小于) 第二个Tuple中对应的元素，则第一个Tuple大于 (小于) 第二个Tuple；否则 (即两个元素相等) ，继续比较下一个元素。

示例：

```sql
SELECT (1, 'z') > (1, 'a') c1, (2022, 01, 02) > (2023, 04, 02) c2, (1,2,3) = (3,2,1) c3;
```

```text
┌─c1─┬─c2─┬─c3─┐
│  1 │  0 │  0 │
└────┴────┴────┘
```

实际应用示例：

```sql
CREATE TABLE test
(
    `year` Int16,
    `month` Int8,
    `day` Int8
)
ENGINE = Memory AS
SELECT *
FROM values((2022, 12, 31), (2000, 1, 1));

SELECT * FROM test;

┌─year─┬─month─┬─day─┐
│ 2022 │    12 │  31 │
│ 2000 │     1 │   1 │
└──────┴───────┴─────┘

SELECT *
FROM test
WHERE (year, month, day) > (2010, 1, 1);

┌─year─┬─month─┬─day─┐
│ 2022 │    12 │  31 │
└──────┴───────┴─────┘
CREATE TABLE test
(
    `key` Int64,
    `duration` UInt32,
    `value` Float64
)
ENGINE = Memory AS
SELECT *
FROM values((1, 42, 66.5), (1, 42, 70), (2, 1, 10), (2, 2, 0));

SELECT * FROM test;

┌─key─┬─duration─┬─value─┐
│   1 │       42 │  66.5 │
│   1 │       42 │    70 │
│   2 │        1 │    10 │
│   2 │        2 │     0 │
└─────┴──────────┴───────┘

-- Let's find a value for each key with the biggest duration, if durations are equal, select the biggest value

SELECT
    key,
    max(duration),
    argMax(value, (duration, value))
FROM test
GROUP BY key
ORDER BY key ASC;

┌─key─┬─max(duration)─┬─argMax(value, tuple(duration, value))─┐
│   1 │            42 │                                    70 │
│   2 │             2 │                                     0 │
└─────┴───────────────┴───────────────────────────────────────┘
```

<div id="nullable-tuple">
  ## Nullable(Tuple(T1, T2, ...))
</div>

:::note Beta 功能
需要 `SET enable_nullable_tuple_type = 1`
这是一个 Beta 功能。
:::

允许整个 Tuple 为 `NULL`；而 `Tuple(Nullable(T1), Nullable(T2), ...)` 则只有各个元素可以为 `NULL`。

| 类型                                         | Tuple 可为 NULL | 元素可为 NULL |
| ------------------------------------------ | ------------- | --------- |
| `Nullable(Tuple(String, Int64))`           | ✅             | ❌         |
| `Tuple(Nullable(String), Nullable(Int64))` | ❌             | ✅         |

示例：

```sql
SET enable_nullable_tuple_type = 1;

CREATE TABLE test (
    id UInt32,
    data Nullable(Tuple(String, Int64))
) ENGINE = Memory;

INSERT INTO test VALUES (1, ('hello', 42)), (2, NULL);

SELECT * FROM test WHERE data IS NULL;
```

```txt
 ┌─id─┬─data─┐
 │  2 │ ᴺᵁᴸᴸ │
 └────┴──────┘
```