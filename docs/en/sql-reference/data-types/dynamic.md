---
slug: /en/sql-reference/data-types/dynamic
sidebar_position: 56
sidebar_label: Dynamic
---

# Dynamic

This type allows to store values of any type inside it without knowing all of them in advance.

To declare a column of `Dynamic` type, use the following syntax:

``` sql
<column_name> Dynamic(max_types=N)
```

Where `N` is an optional parameter between `1` and `255` indicating how many different data types can be stored inside a column with type `Dynamic` across single block of data that is stored separately (for example across single data part for MergeTree table). If this limit is exceeded, all new types will be converted to type `String`. Default value of `max_types` is `32`.

:::note
The Dynamic data type is an experimental feature. To use it, set `allow_experimental_dynamic_type = 1`.
:::

## Creating Dynamic

Using `Dynamic` type in table column definition:

```sql
CREATE TABLE test (d Dynamic) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT d, dynamicType(d) FROM test;
```

```text
┌─d─────────────┬─dynamicType(d)─┐
│ ᴺᵁᴸᴸ          │ None           │
│ 42            │ Int64          │
│ Hello, World! │ String         │
│ [1,2,3]       │ Array(Int64)   │
└───────────────┴────────────────┘
```

Using CAST from ordinary column:

```sql
SELECT 'Hello, World!'::Dynamic as d, dynamicType(d);
```

```text
┌─d─────────────┬─dynamicType(d)─┐
│ Hello, World! │ String         │
└───────────────┴────────────────┘
```

Using CAST from `Variant` column:

```sql
SET allow_experimental_variant_type = 1, use_variant_as_common_type = 1;
SELECT multiIf((number % 3) = 0, number, (number % 3) = 1, range(number + 1), NULL)::Dynamic AS d, dynamicType(d) FROM numbers(3)
```

```text
┌─d─────┬─dynamicType(d)─┐
│ 0     │ UInt64         │
│ [0,1] │ Array(UInt64)  │
│ ᴺᵁᴸᴸ  │ None           │
└───────┴────────────────┘
```


## Reading Dynamic nested types as subcolumns

`Dynamic` type supports reading a single nested type from a `Dynamic` column using the type name as a subcolumn.
So, if you have column `d Dynamic` you can read a subcolumn of any valid type `T` using syntax `d.T`,
this subcolumn will have type `Nullable(T)` if `T` can be inside `Nullable` and `T` otherwise. This subcolumn will
be the same size as original `Dynamic` column and will contain `NULL` values (or empty values if `T` cannot be inside `Nullable`)
in all rows in which original `Dynamic` column doesn't have type `T`.

`Dynamic` subcolumns can be also read using function `dynamicElement(dynamic_column, type_name)`.

Examples:

```sql
CREATE TABLE test (d Dynamic) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT d, dynamicType(d), d.String, d.Int64, d.`Array(Int64)`, d.Date, d.`Array(String)` FROM test;
```

```text
┌─d─────────────┬─dynamicType(d)─┬─d.String──────┬─d.Int64─┬─d.Array(Int64)─┬─d.Date─┬─d.Array(String)─┐
│ ᴺᵁᴸᴸ          │ None           │ ᴺᵁᴸᴸ          │    ᴺᵁᴸᴸ │ []             │   ᴺᵁᴸᴸ │ []              │
│ 42            │ Int64          │ ᴺᵁᴸᴸ          │      42 │ []             │   ᴺᵁᴸᴸ │ []              │
│ Hello, World! │ String         │ Hello, World! │    ᴺᵁᴸᴸ │ []             │   ᴺᵁᴸᴸ │ []              │
│ [1,2,3]       │ Array(Int64)   │ ᴺᵁᴸᴸ          │    ᴺᵁᴸᴸ │ [1,2,3]        │   ᴺᵁᴸᴸ │ []              │
└───────────────┴────────────────┴───────────────┴─────────┴────────────────┴────────┴─────────────────┘
```

```sql
SELECT toTypeName(d.String), toTypeName(d.Int64), toTypeName(d.`Array(Int64)`), toTypeName(d.Date), toTypeName(d.`Array(String)`)  FROM test LIMIT 1;
```

```text
┌─toTypeName(d.String)─┬─toTypeName(d.Int64)─┬─toTypeName(d.Array(Int64))─┬─toTypeName(d.Date)─┬─toTypeName(d.Array(String))─┐
│ Nullable(String)     │ Nullable(Int64)     │ Array(Int64)               │ Nullable(Date)     │ Array(String)               │
└──────────────────────┴─────────────────────┴────────────────────────────┴────────────────────┴─────────────────────────────┘
```

```sql
SELECT d, dynamicType(d), dynamicElement(d, 'String'), dynamicElement(d, 'Int64'), dynamicElement(d, 'Array(Int64)'), dynamicElement(d, 'Date'), dynamicElement(d, 'Array(String)') FROM test;```
```

```text
┌─d─────────────┬─dynamicType(d)─┬─dynamicElement(d, 'String')─┬─dynamicElement(d, 'Int64')─┬─dynamicElement(d, 'Array(Int64)')─┬─dynamicElement(d, 'Date')─┬─dynamicElement(d, 'Array(String)')─┐
│ ᴺᵁᴸᴸ          │ None           │ ᴺᵁᴸᴸ                        │                       ᴺᵁᴸᴸ │ []                                │                      ᴺᵁᴸᴸ │ []                                 │
│ 42            │ Int64          │ ᴺᵁᴸᴸ                        │                         42 │ []                                │                      ᴺᵁᴸᴸ │ []                                 │
│ Hello, World! │ String         │ Hello, World!               │                       ᴺᵁᴸᴸ │ []                                │                      ᴺᵁᴸᴸ │ []                                 │
│ [1,2,3]       │ Array(Int64)   │ ᴺᵁᴸᴸ                        │                       ᴺᵁᴸᴸ │ [1,2,3]                           │                      ᴺᵁᴸᴸ │ []                                 │
└───────────────┴────────────────┴─────────────────────────────┴────────────────────────────┴───────────────────────────────────┴───────────────────────────┴────────────────────────────────────┘
```

To know what variant is stored in each row function `dynamicType(dynamic_column)` can be used. It returns `String` with value type name for each row (or `'None'` if row is `NULL`).

Example:

```sql
CREATE TABLE test (d Dynamic) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT dynamicType(d) from test;
```

```text
┌─dynamicType(d)─┐
│ None           │
│ Int64          │
│ String         │
│ Array(Int64)   │
└────────────────┘
```

## Conversion between Dynamic column and other columns

There are 4 possible conversions that can be performed with `Dynamic` column.

### Converting an ordinary column to a Dynamic column

```sql
SELECT 'Hello, World!'::Dynamic as d, dynamicType(d);
```

```text
┌─d─────────────┬─dynamicType(d)─┐
│ Hello, World! │ String         │
└───────────────┴────────────────┘
```

### Converting a String column to a Dynamic column through parsing

To parse `Dynamic` type values from a `String` column you can enable setting `cast_string_to_dynamic_use_inference`:

```sql
SET cast_string_to_dynamic_use_inference = 1;
SELECT CAST(materialize(map('key1', '42', 'key2', 'true', 'key3', '2020-01-01')), 'Map(String, Dynamic)') as map_of_dynamic, mapApply((k, v) -> (k, dynamicType(v)), map_of_dynamic) as map_of_dynamic_types;
```

```text
┌─map_of_dynamic──────────────────────────────┬─map_of_dynamic_types─────────────────────────┐
│ {'key1':42,'key2':true,'key3':'2020-01-01'} │ {'key1':'Int64','key2':'Bool','key3':'Date'} │
└─────────────────────────────────────────────┴──────────────────────────────────────────────┘
```

### Converting a Dynamic column to an ordinary column

It is possible to convert a `Dynamic` column to an ordinary column. In this case all nested types will be converted to a destination type:

```sql
CREATE TABLE test (d Dynamic) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('42.42'), (true), ('e10');
SELECT d::Nullable(Float64) FROM test;
```

```text
┌─CAST(d, 'Nullable(Float64)')─┐
│                         ᴺᵁᴸᴸ │
│                           42 │
│                        42.42 │
│                            1 │
│                            0 │
└──────────────────────────────┘
```

### Converting a Variant column to Dynamic column

```sql
CREATE TABLE test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('String'), ([1, 2, 3]);
SELECT v::Dynamic as d, dynamicType(d) from test; 
```

```text
┌─d───────┬─dynamicType(d)─┐
│ ᴺᵁᴸᴸ    │ None           │
│ 42      │ UInt64         │
│ String  │ String         │
│ [1,2,3] │ Array(UInt64)  │
└─────────┴────────────────┘
```

### Converting a Dynamic(max_types=N) column to another Dynamic(max_types=K)

If `K >= N` than during conversion the data doesn't change:

```sql
CREATE TABLE test (d Dynamic(max_types=3)) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), (43), ('42.42'), (true);
SELECT d::Dynamic(max_types=5) as d2, dynamicType(d2) FROM test;
```

```text
┌─d─────┬─dynamicType(d)─┐
│ ᴺᵁᴸᴸ  │ None           │
│ 42    │ Int64          │
│ 43    │ Int64          │
│ 42.42 │ String         │
│ true  │ Bool           │
└───────┴────────────────┘
```

If `K < N`, then the values with the rarest types are converted to `String`:
```text
CREATE TABLE test (d Dynamic(max_types=4)) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), (43), ('42.42'), (true), ([1, 2, 3]);
SELECT d, dynamicType(d), d::Dynamic(max_types=2) as d2, dynamicType(d2) FROM test;
```

```text
┌─d───────┬─dynamicType(d)─┬─d2──────┬─dynamicType(d2)─┐
│ ᴺᵁᴸᴸ    │ None           │ ᴺᵁᴸᴸ    │ None            │
│ 42      │ Int64          │ 42      │ Int64           │
│ 43      │ Int64          │ 43      │ Int64           │
│ 42.42   │ String         │ 42.42   │ String          │
│ true    │ Bool           │ true    │ String          │
│ [1,2,3] │ Array(Int64)   │ [1,2,3] │ String          │
└─────────┴────────────────┴─────────┴─────────────────┘
```

If `K=1`, all types are converted to `String`:

```text
CREATE TABLE test (d Dynamic(max_types=4)) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), (43), ('42.42'), (true), ([1, 2, 3]);
SELECT d, dynamicType(d), d::Dynamic(max_types=1) as d2, dynamicType(d2) FROM test;
```

```text
┌─d───────┬─dynamicType(d)─┬─d2──────┬─dynamicType(d2)─┐
│ ᴺᵁᴸᴸ    │ None           │ ᴺᵁᴸᴸ    │ None            │
│ 42      │ Int64          │ 42      │ String          │
│ 43      │ Int64          │ 43      │ String          │
│ 42.42   │ String         │ 42.42   │ String          │
│ true    │ Bool           │ true    │ String          │
│ [1,2,3] │ Array(Int64)   │ [1,2,3] │ String          │
└─────────┴────────────────┴─────────┴─────────────────┘
```

## Reading Dynamic type from the data

All text formats (TSV, CSV, CustomSeparated, Values, JSONEachRow, etc) supports reading `Dynamic` type. During data parsing ClickHouse tries to infer the type of each value and use it during insertion to `Dynamic` column. 

Example:

```sql
SELECT
    d,
    dynamicType(d),
    dynamicElement(d, 'String') AS str,
    dynamicElement(d, 'Int64') AS num,
    dynamicElement(d, 'Float64') AS float,
    dynamicElement(d, 'Date') AS date,
    dynamicElement(d, 'Array(Int64)') AS arr
FROM format(JSONEachRow, 'd Dynamic', $$
{"d" : "Hello, World!"},
{"d" : 42},
{"d" : 42.42},
{"d" : "2020-01-01"},
{"d" : [1, 2, 3]}
$$)
```

```text
┌─d─────────────┬─dynamicType(d)─┬─str───────────┬──num─┬─float─┬───────date─┬─arr─────┐
│ Hello, World! │ String         │ Hello, World! │ ᴺᵁᴸᴸ │  ᴺᵁᴸᴸ │       ᴺᵁᴸᴸ │ []      │
│ 42            │ Int64          │ ᴺᵁᴸᴸ          │   42 │  ᴺᵁᴸᴸ │       ᴺᵁᴸᴸ │ []      │
│ 42.42         │ Float64        │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ │ 42.42 │       ᴺᵁᴸᴸ │ []      │
│ 2020-01-01    │ Date           │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ │  ᴺᵁᴸᴸ │ 2020-01-01 │ []      │
│ [1,2,3]       │ Array(Int64)   │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ │  ᴺᵁᴸᴸ │       ᴺᵁᴸᴸ │ [1,2,3] │
└───────────────┴────────────────┴───────────────┴──────┴───────┴────────────┴─────────┘
```

## Comparing values of Dynamic type

Values of `Dynamic` types are compared similar to values of `Variant` type:
The result of operator `<` for values `d1` with underlying type `T1` and `d2` with underlying type `T2`  of a type `Dynamic` is defined as follows:
- If `T1 = T2 = T`, the result will be `d1.T < d2.T` (underlying values will be compared).
- If `T1 != T2`, the result will be `T1 < T2` (type names will be compared).

Examples:
```sql
CREATE TABLE test (d1 Dynamic, d2 Dynamic) ENGINE=Memory;
INSERT INTO test VALUES (42, 42), (42, 43), (42, 'abc'), (42, [1, 2, 3]), (42, []), (42, NULL);
```

```sql
SELECT d2, dynamicType(d2) as d2_type from test order by d2;
```

```text
┌─d2──────┬─d2_type──────┐
│ []      │ Array(Int64) │
│ [1,2,3] │ Array(Int64) │
│ 42      │ Int64        │
│ 43      │ Int64        │
│ abc     │ String       │
│ ᴺᵁᴸᴸ    │ None         │
└─────────┴──────────────┘
```

```sql
SELECT d1, dynamicType(d1) as d1_type, d2, dynamicType(d2) as d2_type, d1 = d2, d1 < d2, d1 > d2 from test;
```

```text
┌─d1─┬─d1_type─┬─d2──────┬─d2_type──────┬─equals(d1, d2)─┬─less(d1, d2)─┬─greater(d1, d2)─┐
│ 42 │ Int64   │ 42      │ Int64        │              1 │            0 │               0 │
│ 42 │ Int64   │ 43      │ Int64        │              0 │            1 │               0 │
│ 42 │ Int64   │ abc     │ String       │              0 │            1 │               0 │
│ 42 │ Int64   │ [1,2,3] │ Array(Int64) │              0 │            0 │               1 │
│ 42 │ Int64   │ []      │ Array(Int64) │              0 │            0 │               1 │
│ 42 │ Int64   │ ᴺᵁᴸᴸ    │ None         │              0 │            1 │               0 │
└────┴─────────┴─────────┴──────────────┴────────────────┴──────────────┴─────────────────┘
```

If you need to find the row with specific `Dynamic` value, you can do one of the following:

- Cast value to the `Dynamic` type:

```sql
SELECT * FROM test WHERE d2 == [1,2,3]::Array(UInt32)::Dynamic;
```

```text
┌─d1─┬─d2──────┐
│ 42 │ [1,2,3] │
└────┴─────────┘
```

- Compare `Dynamic` subcolumn with required type:

```sql
SELECT * FROM test WHERE d2.`Array(Int65)` == [1,2,3] -- or using variantElement(d2, 'Array(UInt32)')
```

```text
┌─d1─┬─d2──────┐
│ 42 │ [1,2,3] │
└────┴─────────┘
```

Sometimes it can be useful to make additional check on dynamic type as subcolumns with complex types like `Array/Map/Tuple` cannot be inside `Nullable` and will have default values instead of `NULL` on rows with different types:

```sql
SELECT d2, d2.`Array(Int64)`, dynamicType(d2) FROM test WHERE d2.`Array(Int64)` == [];
```

```text
┌─d2───┬─d2.Array(UInt32)─┬─dynamicType(d2)─┐
│ 42   │ []               │ Int64           │
│ 43   │ []               │ Int64           │
│ abc  │ []               │ String          │
│ []   │ []               │ Array(Int32)    │
│ ᴺᵁᴸᴸ │ []               │ None            │
└──────┴──────────────────┴─────────────────┘
```

```sql
SELECT d2, d2.`Array(Int64)`, dynamicType(d2) FROM test WHERE dynamicType(d2) == 'Array(Int64)' AND d2.`Array(Int64)` == [];
```

```text
┌─d2─┬─d2.Array(UInt32)─┬─dynamicType(d2)─┐
│ [] │ []               │ Array(Int64)    │
└────┴──────────────────┴─────────────────┘
```

**Note:** values of dynamic types with different numeric types are considered as different values and not compared between each other, their type names are compared instead.

Example:

```sql
CREATE TABLE test (d Dynamic) ENGINE=Memory;
INSERT INTO test VALUES (1::UInt32), (1::Int64), (100::UInt32), (100::Int64);
SELECT d, dynamicType(d) FROM test ORDER by d;
```

```text
┌─v───┬─dynamicType(v)─┐
│ 1   │ Int64          │
│ 100 │ Int64          │
│ 1   │ UInt32         │
│ 100 │ UInt32         │
└─────┴────────────────┘
```

## Reaching the limit in number of different data types stored inside Dynamic

`Dynamic` data type can store only limited number of different data types inside. By default, this limit is 32, but you can change it in type declaration using syntax `Dynamic(max_types=N)` where N is between 1 and 255 (due to implementation details, it's impossible to have more than 255 different data types inside Dynamic).
When the limit is reached, all new data types inserted to `Dynamic` column will be casted to `String` and stored as `String` values.

Let's see what happens when the limit is reached in different scenarios.

### Reaching the limit during data parsing

During parsing of `Dynamic` values from the data, when the limit is reached for current block of data, all new values will be inserted as `String` values:

```sql
SELECT d, dynamicType(d) FROM format(JSONEachRow, 'd Dynamic(max_types=3)', '
{"d" : 42}
{"d" : [1, 2, 3]}
{"d" : "Hello, World!"}
{"d" : "2020-01-01"}
{"d" : ["str1", "str2", "str3"]}
{"d" : {"a" : 1, "b" : [1, 2, 3]}}
')
```

```text
┌─d──────────────────────────┬─dynamicType(d)─┐
│ 42                         │ Int64          │
│ [1,2,3]                    │ Array(Int64)   │
│ Hello, World!              │ String         │
│ 2020-01-01                 │ String         │
│ ["str1", "str2", "str3"]   │ String         │
│ {"a" : 1, "b" : [1, 2, 3]} │ String         │
└────────────────────────────┴────────────────┘
```

As we can see, after inserting 3 different data types `Int64`, `Array(Int64)` and `String` all new types were converted to `String`.

### During merges of data parts in MergeTree table engines

During merge of several data parts in MergeTree table the `Dynamic` column in the resulting data part can reach the limit of different data types inside and won't be able to store all types from source parts.
In this case ClickHouse chooses what types will remain after merge and what types will be casted to `String`.  In most cases ClickHouse tries to keep the most frequent types and cast the rarest types to `String`, but it depends on the implementation.

Let's see an example of such merge. First, let's create a table with `Dynamic` column, set the limit of different data types to `3` and insert values with `5` different types:

```sql
CREATE TABLE test (id UInt64, d Dynamic(max_types=3)) engine=MergeTree ORDER BY id;
SYSTEM STOP MERGES test;
INSERT INTO test SELECT number, number FROM numbers(5);
INSERT INTO test SELECT number, range(number) FROM numbers(4);
INSERT INTO test SELECT number, toDate(number) FROM numbers(3);
INSERT INTO test SELECT number, map(number, number) FROM numbers(2);
INSERT INTO test SELECT number, 'str_' || toString(number) FROM numbers(1);
```

Each insert will create a separate data pert with `Dynamic` column containing single type:
```sql
SELECT count(), dynamicType(d), _part FROM test GROUP BY _part, dynamicType(d) ORDER BY _part;
```

```text
┌─count()─┬─dynamicType(d)──────┬─_part─────┐
│       5 │ UInt64              │ all_1_1_0 │
│       4 │ Array(UInt64)       │ all_2_2_0 │
│       3 │ Date                │ all_3_3_0 │
│       2 │ Map(UInt64, UInt64) │ all_4_4_0 │
│       1 │ String              │ all_5_5_0 │
└─────────┴─────────────────────┴───────────┘
```

Now, let's merge all parts into one and see what will happen:

```sql
SYSTEM START MERGES test;
OPTIMIZE TABLE test FINAL;
SELECT count(), dynamicType(d), _part FROM test GROUP BY _part, dynamicType(d) ORDER BY _part;
```

```text
┌─count()─┬─dynamicType(d)─┬─_part─────┐
│       5 │ UInt64         │ all_1_5_2 │
│       6 │ String         │ all_1_5_2 │
│       4 │ Array(UInt64)  │ all_1_5_2 │
└─────────┴────────────────┴───────────┘
```

As we can see, ClickHouse kept the most frequent types `UInt64` and `Array(UInt64)` and casted all other types to `String`.
