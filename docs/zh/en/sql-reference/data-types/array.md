---
description: 'ClickHouse 中 Array 数据类型的文档'
sidebar_label: 'Array(T)'
sidebar_position: 32
slug: /sql-reference/data-types/array
title: 'Array(T)'
doc_type: 'reference'
---

由 `T` 类型元素组成的数组，数组索引从 1 开始。`T` 可以是任何数据类型，包括数组。

<div id="creating-an-array">
  ## 创建数组
</div>

可以使用函数创建数组：

```sql
array(T)
```

你也可以使用 `[]`。

```sql
[]
```

创建数组示例：

```sql
SELECT array(1, 2) AS x, toTypeName(x)
```

```text
┌─x─────┬─toTypeName(array(1, 2))─┐
│ [1,2] │ Array(UInt8)            │
└───────┴─────────────────────────┘
```

```sql
SELECT [1, 2] AS x, toTypeName(x)
```

```text
┌─x─────┬─toTypeName([1, 2])─┐
│ [1,2] │ Array(UInt8)       │
└───────┴────────────────────┘
```

<div id="working-with-data-types">
  ## 使用数据类型
</div>

在动态创建数组时，ClickHouse 会自动将参数类型定义为能够存储所有已列出参数的最窄数据类型。如果存在任何 [Nullable](/zh/sql-reference/data-types/nullable) 或字面量 [NULL](/zh/operations/settings/formats#input_format_null_as_default) 值，数组元素的类型也会变为 [Nullable](../../sql-reference/data-types/nullable.md)。

如果 ClickHouse 无法确定数据类型，则会抛出异常。例如，在尝试同时使用字符串和数字创建数组时，就会发生这种情况 (`SELECT array(1, 'a')`) 。

自动数据类型检测示例：

```sql
SELECT array(1, 2, NULL) AS x, toTypeName(x)
```

```text
┌─x──────────┬─toTypeName(array(1, 2, NULL))─┐
│ [1,2,NULL] │ Array(Nullable(UInt8))        │
└────────────┴───────────────────────────────┘
```

如果您尝试创建包含不兼容数据类型的数组，ClickHouse 会抛出异常：

```sql
SELECT array(1, 'a')
```

```text
Received exception from server (version 1.1.54388):
Code: 386. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: There is no supertype for types UInt8, String because some of them are String/FixedString and some of them are not.
```

<div id="array-size">
  ## Array 的大小
</div>

可以使用 `size0` 子列来获取数组大小，无需读取整列。对于多维数组，可以使用 `sizeN-1`，其中 `N` 为所需的维度。

**示例**

```sql title="Query"
CREATE TABLE t_arr (`arr` Array(Array(Array(UInt32)))) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_arr VALUES ([[[12, 13, 0, 1],[12]]]);

SELECT arr.size0, arr.size1, arr.size2 FROM t_arr;
```

```text title="Response"
┌─arr.size0─┬─arr.size1─┬─arr.size2─┐
│         1 │ [2]       │ [[4,1]]   │
└───────────┴───────────┴───────────┘
```

<div id="reading-nested-subcolumns-from-array">
  ## 从 Array 读取嵌套子列
</div>

如果 `Array` 中的嵌套类型 `T` 有子列 (例如，它是一个[命名元组](./tuple.md)) ，则可以从 `Array(T)` 类型中以相同的子列名读取这些子列。子列的类型将是 `Array`，其元素类型为原始子列的类型。

**示例**

```sql
CREATE TABLE t_arr (arr Array(Tuple(field1 UInt32, field2 String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_arr VALUES ([(1, 'Hello'), (2, 'World')]), ([(3, 'This'), (4, 'is'), (5, 'subcolumn')]);
SELECT arr.field1, toTypeName(arr.field1), arr.field2, toTypeName(arr.field2) from t_arr;
```

```test
┌─arr.field1─┬─toTypeName(arr.field1)─┬─arr.field2────────────────┬─toTypeName(arr.field2)─┐
│ [1,2]      │ Array(UInt32)          │ ['Hello','World']         │ Array(String)          │
│ [3,4,5]    │ Array(UInt32)          │ ['This','is','subcolumn'] │ Array(String)          │
└────────────┴────────────────────────┴───────────────────────────┴────────────────────────┘
```