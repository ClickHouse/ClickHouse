# Tuple(T1, T2, ...)

元组，元组中的每个元素都有独立的[类型](index.md#data_types)。

元组不能写入表中（除 Memory 表外）。它们会用于临时列分组。在某个查询中，IN 表达式和带特定参数的 lambda 函数可以来对临时列进行分组。更多信息，参见 [IN 操作符] (../query_language/select.md#in_operators)  和 [高阶函数](../query_language/functions/higher_order_functions.md#higher_order_functions)。

## 创建元组

可以用下面函数来创建元组：

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

## 元组中的数据类型

当动态创建元组时，ClickHouse 会自动为元组的每一个参数赋予最小可表达的类型。如果该参数是 [NULL](../query_language/syntax.md#null-literal)，那么这个元组对应元素就是 [Nullable](nullable.md#data_type-nullable)。

自动识别数据类型的示例：

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


[来源文章](https://clickhouse.yandex/docs/en/data_types/tuple/) <!--hide-->

