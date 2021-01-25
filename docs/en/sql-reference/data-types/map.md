---
toc_priority: 65
toc_title: Map(key, value)
---

# Map(key, value) {#data_type-map}

`Map(key, value)` data type stores `key:value` pairs in structures like JSON. 

**Parameters** 
-   `key` — Key part of the pair. [String](../../sql-reference/data-types/string.md) or [Integer](../../sql-reference/data-types/int-uint.md).
-   `value` — Value part of the pair. [String](../../sql-reference/data-types/string.md), [Integer](../../sql-reference/data-types/int-uint.md) or [Array](../../sql-reference/data-types/array.md).

!!! warning "Warning"
    Currently `Map` data type is an experimental feature. To work with it you must set `allow_experimental_map_type = 1`.

To get the value from an `a Map('key', 'value')` column, use `a['key']` syntax.

**Example**

Query:

``` sql
CREATE TABLE table_map (a Map(String, UInt64)) ENGINE=Memory;
INSERT INTO table_map VALUES ({'key1':1, 'key2':100}), ({'key1':2,'key2':200}), ({'key1':3,'key2':300});
SELECT a['key2'] FROM table_map;
```
Result:

```text
┌─arrayElement(a, 'key2')─┐
│                     100 │
│                     200 │
│                     300 │
└─────────────────────────┘
```

## Map() and Tuple() Types {#map-and-tuple}

You can cast `Tuple()` as `Map()`:

``` sql
SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map;
```

``` text
┌─map───────────────────────────┐
│ {1:'Ready',2:'Steady',3:'Go'} │
└───────────────────────────────┘
```

**See Also**

-   [map()](../../sql-reference/functions/tuple-map-functions.md#function-map) function

[Original article](https://clickhouse.tech/docs/en/data_types/map/) <!--hide-->
