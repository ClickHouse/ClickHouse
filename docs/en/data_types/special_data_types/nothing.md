<a name="special_data_type-nothing"></a>

# Nothing

The only purpose of this data type is to represent [NULL](../../query_language/syntax.md#null-literal), i.e., no value.

You can't create a `Nothing` type value, because it is used where a value is not expected. For example, `NULL` is written as `Nullable(Nothing)` ([Nullable](../../data_types/nullable.md#data_type-nullable) — this is the data type that allows storing `NULL` in tables.) The `Nothing` type is also used to denote empty arrays:

```bash
:) SELECT toTypeName(Array())

SELECT toTypeName([])

┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘

1 rows in set. Elapsed: 0.062 sec.
```

