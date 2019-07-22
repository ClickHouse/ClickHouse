
# Nothing

The only purpose of this data type is to represent cases where value is not expected. So you can't create a `Nothing` type value.

For example, literal [NULL](../../query_language/syntax.md#null-literal) has type of `Nullable(Nothing)`. See more about [Nullable](../../data_types/nullable.md).

The `Nothing` type can also used to denote empty arrays:

```bash
:) SELECT toTypeName(array())

SELECT toTypeName([])

┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘

1 rows in set. Elapsed: 0.062 sec.
```


[Original article](https://clickhouse.yandex/docs/en/data_types/special_data_types/nothing/) <!--hide-->
