<a name="special_data_type-nothing"></a>

# Nothing

此数据类型的唯一目的是表示不是期望值的情况。 所以你不能创建一个` Nothing` 类型的值。

例如， [NULL](../../query_language/syntax.md#null-literal) 有 `Nullable(Nothing)`。更多参见[Nullable](../../data_types/nullable.md#data_type-nullable).

`Nothing` 类型也可用于表示空数组：

```bash
:) SELECT toTypeName(array())

SELECT toTypeName([])

┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘

1 rows in set. Elapsed: 0.062 sec.
```


[来源文章](https://clickhouse.yandex/docs/en/data_types/special_data_types/nothing/) <!--hide-->
