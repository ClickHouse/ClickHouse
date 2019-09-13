# Array(T) {#data_type-array}

Array of `T`-type items.

`T` can be anything, including an array.

## Creating an array

You can use a function to create an array:

```sql
array(T)
```

You can also use square brackets.

```sql
[]
```

Example of creating an array:

```
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

## Working with data types

When creating an array on the fly, ClickHouse automatically defines the argument type as the narrowest data type that can store all the listed arguments. If there are any [NULL](../query_language/syntax.md#null-literal) or [Nullable](nullable.md#data_type-nullable) type arguments, the type of array elements is [Nullable](nullable.md).

If ClickHouse couldn't determine the data type, it will generate an exception. For instance, this will happen when trying to create an array with strings and numbers simultaneously (`SELECT array(1, 'a')`).

Examples of automatic data type detection:

```sql
SELECT array(1, 2, NULL) AS x, toTypeName(x)
```    
```text
┌─x──────────┬─toTypeName(array(1, 2, NULL))─┐
│ [1,2,NULL] │ Array(Nullable(UInt8))        │
└────────────┴───────────────────────────────┘
```

If you try to create an array of incompatible data types, ClickHouse throws an exception:

```sql
SELECT array(1, 'a')
```


[Original article](https://clickhouse.yandex/docs/en/data_types/array/) <!--hide-->
