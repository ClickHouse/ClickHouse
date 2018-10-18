<a name="data_type-array"></a>

# Array(T)

由`T`类型元素组成的数组

`T`可以是任意元素，包含数组谨慎使用多维数组ClickHouse对多维数组的支持有限例如，它们不能存储在`MergeTree`表中

## 创建数组

您可以使用一个函数创建数组：

```
array(T)
```

您也可以使用方括号：

```
[]
```

创建数组示例：

```
:) SELECT array(1, 2) AS x, toTypeName(x)

SELECT
    [1, 2] AS x,
    toTypeName(x)

┌─x─────┬─toTypeName(array(1, 2))─┐
│ [1,2] │ Array(UInt8)            │
└───────┴─────────────────────────┘

1 rows in set. Elapsed: 0.002 sec.

:) SELECT [1, 2] AS x, toTypeName(x)

SELECT
    [1, 2] AS x,
    toTypeName(x)

┌─x─────┬─toTypeName([1, 2])─┐
│ [1,2] │ Array(UInt8)       │
└───────┴────────────────────┘

1 rows in set. Elapsed: 0.002 sec.
```

## 使用数据类型

当动态创建数组时，ClickHouse自动将参数类型定义为可以存储所有列出的参数的最窄的数据类型如果存在任何 [NULL](../query_language/syntax.md#null-literal) 或者  [Nullable](nullable.md#data_type-nullable) 类型参数，那么数组元素的类型是 [Nullable](nullable.md#data_type-nullable)

如果ClickHouse无法确定数据类型，它将产生异常当尝试同时创建一个包含字符串和数字的数组时会发生这种情况 (`SELECT array(1, 'a')`)

自动数据类型检测示例：

```
:) SELECT array(1, 2, NULL) AS x, toTypeName(x)

SELECT
    [1, 2, NULL] AS x,
    toTypeName(x)

┌─x──────────┬─toTypeName(array(1, 2, NULL))─┐
│ [1,2,NULL] │ Array(Nullable(UInt8))        │
└────────────┴───────────────────────────────┘

1 rows in set. Elapsed: 0.002 sec.
```

如果您尝试创建不兼容的数据类型数组，ClickHouse将引发异常：

```
:) SELECT array(1, 'a')

SELECT [1, 'a']

Received exception from server (version 1.1.54388):
Code: 386. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: There is no supertype for types UInt8, String because some of them are String/FixedString and some of them are not.

0 rows in set. Elapsed: 0.246 sec.
```

