---
toc_priority: 65
toc_title: Map(key, value)
---

# Map(key, value) {#data_type-map}

`Map(key, value)` data type stores `key:value` pairs. 

**Parameters** 

-   `key` — The key part of the pair. [String](../../sql-reference/data-types/string.md) or [Integer](../../sql-reference/data-types/int-uint.md).
-   `value` — The value part of the pair. [String](../../sql-reference/data-types/string.md), [Integer](../../sql-reference/data-types/int-uint.md) or [Array](../../sql-reference/data-types/array.md).

!!! warning "Warning"
    Currently `Map` data type is an experimental feature. To work with it you must set `allow_experimental_map_type = 1`.

To get the value from an `a Map('key', 'value')` column, use `a['key']` syntax. This lookup works now with a linear complexity.

**Examples**

Consider the table:

``` sql
CREATE TABLE table_map (a Map(String, UInt64)) ENGINE=Memory;
INSERT INTO table_map VALUES ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});
```

Select all `key2` values: 

```sql
SELECT a['key2'] FROM table_map;
```
Result:

```text
┌─arrayElement(a, 'key2')─┐
│                      10 │
│                      20 │
│                      30 │
└─────────────────────────┘
```

If there's no such `key` in the `Map()` column, the query returns zeros for numerical values, empty strings or empty arrays. 

```sql
INSERT INTO table_map VALUES ({'key3':100}), ({});
SELECT a['key3'] FROM table_map;
```

Result:

```text
┌─arrayElement(a, 'key3')─┐
│                     100 │
│                       0 │
└─────────────────────────┘
┌─arrayElement(a, 'key3')─┐
│                       0 │
│                       0 │
│                       0 │
└─────────────────────────┘
```

## Convert Tuple to Map Type {#map-and-tuple}

You can cast `Tuple()` as `Map()` using [CAST](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) function:

``` sql
SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map;
```

``` text
┌─map───────────────────────────┐
│ {1:'Ready',2:'Steady',3:'Go'} │
└───────────────────────────────┘
```

## Map.keys and Map.values Subcolumns {#map-subcolumns}

To optimize `Map` column processing, in some cases you can use the `keys` and `values` subcolumns instead of reading the whole column.

**Example**

Query:

``` sql
CREATE TABLE t_map (`a` Map(String, UInt64)) ENGINE = Memory;

INSERT INTO t_map VALUES (map('key1', 1, 'key2', 2, 'key3', 3));

SELECT a.keys FROM t_map;

SELECT a.values FROM t_map;
```

Result:

``` text
┌─a.keys─────────────────┐
│ ['key1','key2','key3'] │
└────────────────────────┘

┌─a.values─┐
│ [1,2,3]  │
└──────────┘
```

**See Also**

-   [map()](../../sql-reference/functions/tuple-map-functions.md#function-map) function
-   [CAST()](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) function

[Original article](https://clickhouse.tech/docs/en/data-types/map/) <!--hide-->
