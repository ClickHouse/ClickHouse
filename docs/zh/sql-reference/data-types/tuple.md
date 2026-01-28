---
slug: /zh/sql-reference/data-types/tuple
---
# Tuple(T1, T2, ...) {#tuplet1-t2}

元组，其中每个元素都有单独的 [类型](index.md#data_types)。

不能在表中存储元组（除了内存表）。它们可以用于临时列分组。在查询中，IN 表达式和带特定参数的 lambda 函数可以来对临时列进行分组。更多信息，请参阅 [IN 操作符](../../sql-reference/data-types/tuple.md) 和 [高阶函数](../../sql-reference/data-types/tuple.md)。

元组可以是查询的结果。在这种情况下，对于JSON以外的文本格式，括号中的值是逗号分隔的。在JSON格式中，元组作为数组输出（在方括号中）。

## 创建元组 {#chuang-jian-yuan-zu}

可以使用函数来创建元组：

    tuple(T1, T2, ...)

创建元组的示例：

```sql
SELECT tuple(1,'a') AS x, toTypeName(x)
```

```response
┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘
```

## 元组中的数据类型 {#yuan-zu-zhong-de-shu-ju-lei-xing}

在动态创建元组时，ClickHouse 会自动为元组的每一个参数赋予最小可表达的类型。如果参数为 [NULL](../../sql-reference/data-types/tuple.md#null-literal)，那这个元组对应元素是 [可为空](nullable.md)。

自动数据类型检测示例：

```sql
SELECT tuple(1, NULL) AS x, toTypeName(x)
```

```response
┌─x────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1,NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└──────────┴─────────────────────────────────┘
```
