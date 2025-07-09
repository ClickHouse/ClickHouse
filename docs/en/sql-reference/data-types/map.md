---
slug: /en/sql-reference/data-types/map
sidebar_position: 36
sidebar_label: Map(K, V)
---

# Map(K, V)

Data type `Map(K, V)` stores key-value pairs.

Unlike other databases, maps are not unique in ClickHouse, i.e. a map can contain two elements with the same key.
(The reason for that is that maps are internally implemented as `Array(Tuple(K, V))`.)

You can use use syntax `m[k]` to obtain the value for key `k` in map `m`.
Also, `m[k]` scans the map, i.e. the runtime of the operation is linear in the size of the map.

**Parameters**

- `K` — The type of the Map keys. Arbitrary type except [Nullable](../../sql-reference/data-types/nullable.md) and [LowCardinality](../../sql-reference/data-types/lowcardinality.md) nested with [Nullable](../../sql-reference/data-types/nullable.md) types.
- `V` — The type of the Map values. Arbitrary type.

**Examples**

Create a table with a column of type map:

``` sql
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});
```

To select `key2` values:

```sql
SELECT m['key2'] FROM tab;
```

Result:

```text
┌─arrayElement(m, 'key2')─┐
│                      10 │
│                      20 │
│                      30 │
└─────────────────────────┘
```

If the requested key `k` is not contained in the map, `m[k]` returns the value type's default value, e.g. `0` for integer types and `''` for string types.
To check whether a key exists in a map, you can use function [mapContains](../../sql-reference/functions/tuple-map-functions#mapcontains).

```sql
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':100}), ({});
SELECT m['key1'] FROM tab;
```

Result:

```text
┌─arrayElement(m, 'key1')─┐
│                     100 │
│                       0 │
└─────────────────────────┘
```

## Converting Tuple to Map

Values of type `Tuple()` can be casted to values of type `Map()` using function [CAST](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast):

**Example**

Query:

``` sql
SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map;
```

Result:

``` text
┌─map───────────────────────────┐
│ {1:'Ready',2:'Steady',3:'Go'} │
└───────────────────────────────┘
```

## Reading subcolumns of Map

To avoid reading the entire map, you can use subcolumns `keys` and `values` in some cases.

**Example**

Query:

``` sql
CREATE TABLE tab (m Map(String, UInt64)) ENGINE = Memory;
INSERT INTO tab VALUES (map('key1', 1, 'key2', 2, 'key3', 3));

SELECT m.keys FROM tab; --   same as mapKeys(m)
SELECT m.values FROM tab; -- same as mapValues(m)
```

Result:

``` text
┌─m.keys─────────────────┐
│ ['key1','key2','key3'] │
└────────────────────────┘

┌─m.values─┐
│ [1,2,3]  │
└──────────┘
```

**See Also**

- [map()](../../sql-reference/functions/tuple-map-functions.md#function-map) function
- [CAST()](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) function
- [-Map combinator for Map datatype](../aggregate-functions/combinators.md#-map)


## Related content

- Blog: [Building an Observability Solution with ClickHouse - Part 2 - Traces](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)
