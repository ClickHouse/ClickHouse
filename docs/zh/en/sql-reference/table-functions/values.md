---
description: '创建一个以值填充列的临时存储。'
keywords: ['values', '表函数']
sidebar_label: 'values'
sidebar_position: 210
slug: /sql-reference/table-functions/values
title: 'values'
doc_type: 'reference'
---

`Values` 表函数可用于创建以值填充列的临时存储。它适合用于快速测试或生成样本数据。

:::note
Values 是一个不区分大小写的函数。也就是说，`VALUES` 和 `values` 都是有效的。
:::

<div id="syntax">
  ## 语法
</div>

`VALUES` 表函数的基本语法如下：

```sql
VALUES([structure,] values...)
```

常见用法如下：

```sql
VALUES(
    ['column1_name Type1, column2_name Type2, ...'],
    (value1_row1, value2_row1, ...),
    (value1_row2, value2_row2, ...),
    ...
)
```

<div id="arguments">
  ## 参数
</div>

* `column1_name Type1, ...` (可选) 。[String](/zh/sql-reference/data-types/string)
  用于指定列名和类型。如果省略该参数，这些列将
  命名为 `c1`、`c2` 等。
* `(value1_row1, value2_row1)`。[Tuple](/zh/sql-reference/data-types/tuple)
  包含任意类型的值。

:::note
也可以将用逗号分隔的元组替换为单个值。在这种情况下，
每个值都会被视为新的一行。详见[示例](#examples)
部分。
:::

<div id="returned-value">
  ## 返回值
</div>

* 返回一个包含给定值的临时表。

<div id="examples">
  ## 示例
</div>

```sql title="Query"
SELECT *
FROM VALUES(
    'person String, place String',
    ('Noah', 'Paris'),
    ('Emma', 'Tokyo'),
    ('Liam', 'Sydney'),
    ('Olivia', 'Berlin'),
    ('Ilya', 'London'),
    ('Sophia', 'London'),
    ('Jackson', 'Madrid'),
    ('Alexey', 'Amsterdam'),
    ('Mason', 'Venice'),
    ('Isabella', 'Prague')
)
```

```response title="Response"
    ┌─person───┬─place─────┐
 1. │ Noah     │ Paris     │
 2. │ Emma     │ Tokyo     │
 3. │ Liam     │ Sydney    │
 4. │ Olivia   │ Berlin    │
 5. │ Ilya     │ London    │
 6. │ Sophia   │ London    │
 7. │ Jackson  │ Madrid    │
 8. │ Alexey   │ Amsterdam │
 9. │ Mason    │ Venice    │
10. │ Isabella │ Prague    │
    └──────────┴───────────┘
```

`VALUES` 也可用于单个值，而不必使用元组。例如：

```sql title="Query"
SELECT *
FROM VALUES(
    'person String',
    'Noah',
    'Emma',
    'Liam',
    'Olivia',
    'Ilya',
    'Sophia',
    'Jackson',
    'Alexey',
    'Mason',
    'Isabella'
)
```

```response title="Response"
    ┌─person───┐
 1. │ Noah     │
 2. │ Emma     │
 3. │ Liam     │
 4. │ Olivia   │
 5. │ Ilya     │
 6. │ Sophia   │
 7. │ Jackson  │
 8. │ Alexey   │
 9. │ Mason    │
10. │ Isabella │
    └──────────┘
```

或者不提供行定义 ([语法](#syntax) 中的 `'column1_name Type1, column2_name Type2, ...'`) ，
此时列名会自动生成。

例如：

```sql title="Query"
-- tuples as values
SELECT *
FROM VALUES(
    ('Noah', 'Paris'),
    ('Emma', 'Tokyo'),
    ('Liam', 'Sydney'),
    ('Olivia', 'Berlin'),
    ('Ilya', 'London'),
    ('Sophia', 'London'),
    ('Jackson', 'Madrid'),
    ('Alexey', 'Amsterdam'),
    ('Mason', 'Venice'),
    ('Isabella', 'Prague')
)
```

```response title="Response"
    ┌─c1───────┬─c2────────┐
 1. │ Noah     │ Paris     │
 2. │ Emma     │ Tokyo     │
 3. │ Liam     │ Sydney    │
 4. │ Olivia   │ Berlin    │
 5. │ Ilya     │ London    │
 6. │ Sophia   │ London    │
 7. │ Jackson  │ Madrid    │
 8. │ Alexey   │ Amsterdam │
 9. │ Mason    │ Venice    │
10. │ Isabella │ Prague    │
    └──────────┴───────────┘
```

```sql title="Query"
-- single values
SELECT *
FROM VALUES(
    'Noah',
    'Emma',
    'Liam',
    'Olivia',
    'Ilya',
    'Sophia',
    'Jackson',
    'Alexey',
    'Mason',
    'Isabella'
)
```

```response title="Response"
    ┌─c1───────┐
 1. │ Noah     │
 2. │ Emma     │
 3. │ Liam     │
 4. │ Olivia   │
 5. │ Ilya     │
 6. │ Sophia   │
 7. │ Jackson  │
 8. │ Alexey   │
 9. │ Mason    │
10. │ Isabella │
    └──────────┘
```

<div id="sql-standard-values-clause">
  ## SQL 标准 `VALUES` 子句
</div>

从 26.3 版本开始，ClickHouse 也支持 SQL 标准的 `VALUES` 子句，可像 PostgreSQL、MySQL、DuckDB 和 SQL Server 那样，在 `FROM` 中作为表表达式使用。该语法在内部会被重写为使用上文所述的 `values` 表函数。

```sql title="Query"
SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val);
```

```response title="Response"
┌─id─┬─val─┐
│  1 │ a   │
│  2 │ b   │
│  3 │ c   │
└────┴─────┘
```

可在 CTE 中使用：

```sql title="Query"
WITH cte AS (SELECT * FROM (VALUES (1, 'one'), (2, 'two')) AS t(id, name))
SELECT * FROM cte;
```

以及在 JOIN 中：

```sql title="Query"
SELECT t1.id, t1.val, t2.val2
FROM (VALUES (1, 'a'), (2, 'b')) AS t1(id, val)
JOIN (VALUES (1, 'x'), (2, 'y')) AS t2(id, val2) ON t1.id = t2.id;
```

:::note
`AS t(col1, col2, ...)` 后面的列别名遵循标准 SQL 语法，用于为派生表的列命名。若省略，则列名将为 `c1`、`c2` 等。
:::

<div id="see-also">
  ## 另请参见
</div>

* [Values 格式](/zh/interfaces/formats/Values)