---
slug: /en/sql-reference/data-types/map
sidebar_position: 36
sidebar_label: Map(K, V)
---

# Map(K, V)

`Map(K, V)` data type stores `key:value` pairs.
The Map datatype is implemented as `Array(Tuple(key T1, value T2))`, which means that the order of keys in each map does not change, i.e., this data type maintains insertion order.

**Parameters**

- `key` — The key part of the pair. Arbitrary type, except [Nullable](../../sql-reference/data-types/nullable.md) and [LowCardinality](../../sql-reference/data-types/lowcardinality.md) nested with [Nullable](../../sql-reference/data-types/nullable.md) types.
- `value` — The value part of the pair. Arbitrary type, including [Map](../../sql-reference/data-types/map.md) and [Array](../../sql-reference/data-types/array.md).

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

## Convert Tuple to Map Type

You can cast `Tuple()` as `Map()` using [CAST](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) function:

``` sql
SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map;
```

``` text
┌─map───────────────────────────┐
│ {1:'Ready',2:'Steady',3:'Go'} │
└───────────────────────────────┘
```

## Map.keys and Map.values Subcolumns

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

- [map()](../../sql-reference/functions/tuple-map-functions.md#function-map) function
- [CAST()](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) function
- [-Map combinator for Map datatype](../aggregate-functions/combinators.md#-map)


## Related content

- Blog: [Building an Observability Solution with ClickHouse - Part 2 - Traces](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)
