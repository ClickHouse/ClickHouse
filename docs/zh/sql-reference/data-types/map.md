---
sidebar_position: 65
sidebar_label: Map(key, value)
---

# Map(key, value) {#data_type-map}

`Map(key, value)` 可以存储 `key:value` 键值对类型的数据。

**参数**

-   `key` — 键值对的key，类型可以是：[String](../../sql-reference/data-types/string.md), [Integer](../../sql-reference/data-types/int-uint.md), [LowCardinality](../../sql-reference/data-types/lowcardinality.md), 或者 [FixedString](../../sql-reference/data-types/fixedstring.md).
-   `value` — 键值对的value，类型可以是：[String](../../sql-reference/data-types/string.md), [Integer](../../sql-reference/data-types/int-uint.md), [Array](../../sql-reference/data-types/array.md), [LowCardinality](../../sql-reference/data-types/lowcardinality.md), 或者 [FixedString](../../sql-reference/data-types/fixedstring.md).

使用 `a['key']` 可以从 `a Map('key', 'value')` 类型的列中获取到对应的值，这是一个线性复杂度的查询。

**示例**

示例表:

``` sql
CREATE TABLE table_map (a Map(String, UInt64)) ENGINE=Memory;
INSERT INTO table_map VALUES ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});
```

查询 `key2` 的所有值:

```sql
SELECT a['key2'] FROM table_map;
```
查询结果:

```text
┌─arrayElement(a, 'key2')─┐
│                      10 │
│                      20 │
│                      30 │
└─────────────────────────┘
```

如果在 `Map()` 类型的列中，查询的 `key` 值不存在，那么根据 `value` 的类型，查询结果将会是数字0，空字符串或者空数组。

```sql
INSERT INTO table_map VALUES ({'key3':100}), ({});
SELECT a['key3'] FROM table_map;
```

查询结果:

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

## 将Tuple类型转换成Map类型 {#map-and-tuple}

您可以使用 [CAST](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) 方法将 `Tuple()` 转换成 `Map()` ：

``` sql
SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map;
```

``` text
┌─map───────────────────────────┐
│ {1:'Ready',2:'Steady',3:'Go'} │
└───────────────────────────────┘
```

## Map.keys 和 Map.values 单独使用 {#map-subcolumns}

为了更好使用 `Map` 类型，在一定的场景下，可以单独使用 `keys` 或者 `values`，而不需要将整个列的数据都读取出来。

**示例**

查询:

``` sql
CREATE TABLE t_map (`a` Map(String, UInt64)) ENGINE = Memory;
INSERT INTO t_map VALUES (map('key1', 1, 'key2', 2, 'key3', 3));
SELECT a.keys FROM t_map;
SELECT a.values FROM t_map;
```

结果:

``` text
┌─a.keys─────────────────┐
│ ['key1','key2','key3'] │
└────────────────────────┘
┌─a.values─┐
│ [1,2,3]  │
└──────────┘
```

**另请参阅**

-   [map()](../../sql-reference/functions/tuple-map-functions.md#function-map) function
-   [CAST()](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) function

[Original article](https://clickhouse.com/docs/zh/sql-reference/data-types/map/) <!--hide-->
