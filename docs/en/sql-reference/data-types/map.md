---
toc_priority: 65
toc_title: Map
---

# Map {#data_type-map}

`Map(value_type)` data type stores `key:value` pairs. The key type is always [String](../../sql-reference/data-types/string.md).

**Parameters** 
-   `value_type` — The value type. Any type that can be nested into [Nullable](../../sql-reference/data-types/nullable.md).

!!! warning "Warning"
    Currently `Map` data type is an experimental feature. To work with it you must set `allow_experimental_map_type = 1`.

To get the value from an `Map` column, use arrayElement `m['key']` syntax. If there's no such key in the `Map` column, the query returns `NULL`. This lookup works now with a const complexity.

**Examples**

Consider the table:

``` sql
SET allow_experimental_map_type = 1;

CREATE TABLE test_map (
  dt Date,
  prices Map(Float64)
) ENGINE = MergeTree
ORDER BY dt;

INSERT INTO test_map(dt, prices) VALUES ('2020-01-01', {'banana': 4.2, 'pear': 3.2}), ('2020-01-02', {'apple': 8.3});
```

Select all `apple` values: 

```sql
SELECT dt, prices['apple'] FROM test_map ORDER BY dt;
```
Result:

```text
┌─────────dt─┬─arrayElement(prices, 'apple')─┐
│ 2020-01-01 │                          ᴺᵁᴸᴸ │
│ 2020-01-02 │                           8.3 │
└────────────┴───────────────────────────────┘
```


## Convert Tuple to Map Type {#cast-tuple-map}

You can cast `Tuple(Array(String), Array(value_type))` as `Map(value_type)` using [CAST](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) function:

``` sql
SELECT CAST((['apple', 'banana', 'pear'], [1.0, 2.0, 3.0]), 'Map(Float64)') AS map;
```

``` text
┌─map─────────────────────────────┐
│ {'apple':1,'banana':2,'pear':3} │
└─────────────────────────────────┘
```

**See Also**

-   [map](../../sql-reference/functions/tuple-map-functions.md#function-map) function
-   [CAST](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) function

[Original article](https://clickhouse.tech/docs/en/data-types/map/) <!--hide-->
