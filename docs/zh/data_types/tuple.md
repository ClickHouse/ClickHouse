<a name="data_type-tuple"></a>

# Tuple(T1, T2, ...)

元组，其中每个元素都有一个单独的 [type](index.md#data_types)

不能在表中存储元组（除了内存表）它们用于临时列分组在查询中使用IN表达式时，可以对列进行分组，以及指定lambda函数的某些形式参数有关更多信息，请参阅 [IN operators](../query_language/select.md#in_operators) and [Higher order functions](../query_language/functions/higher_order_functions.md#higher_order_functions)

元组可以是查询的结果在这种情况下，对于JSON以外的文本格式，括号中的值是逗号分隔的在JSON格式中，元组作为数组输出（在方括号中）

## 创建元组

您可以使用函数来创建元组：

```
tuple(T1, T2, ...)
```

创建元组的示例：

```
:) SELECT tuple(1,'a') AS x, toTypeName(x)

SELECT
    (1, 'a') AS x,
    toTypeName(x)

┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘

1 rows in set. Elapsed: 0.021 sec.
```

## 使用数据类型

在动态创建元组时，ClickHouse会自动检测每个参数的类型，作为可存储参数值的类型的最小值如果参数为 [NULL](../query_language/syntax.md#null-literal)，则元组元素的类型是 [Nullable](nullable.md#data_type-nullable)

自动数据类型检测示例：

```
SELECT tuple(1, NULL) AS x, toTypeName(x)

SELECT
    (1, NULL) AS x,
    toTypeName(x)

┌─x────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1,NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└──────────┴─────────────────────────────────┘

1 rows in set. Elapsed: 0.002 sec.
```

