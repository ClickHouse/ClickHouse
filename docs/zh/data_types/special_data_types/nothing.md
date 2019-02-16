
# Nothing

此数据类型的唯一目的是表示不是期望值的情况。 所以不能创建一个 `Nothing` 类型的值。

例如，文本 [NULL](../../query_language/syntax.md#null-literal) 的类型为 `Nullable(Nothing)`。详情请见 [Nullable](../../data_types/nullable.md)。

`Nothing` 类型也可以用来表示空数组：

```bash
:) SELECT toTypeName(array())

SELECT toTypeName([])

┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘

1 rows in set. Elapsed: 0.062 sec.
```
