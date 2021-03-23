---
toc_priority: 46
toc_title: Working with maps
---

# Functions for maps {#functions-for-working-with-tuple-maps}

## mapBuild {#function-mapbuild}

Arranges `key:value` pairs into [Map](../../sql-reference/data-types/map.md) data type.

**Syntax** 

``` sql
mapBuild(key1, value1[, key2, value2, ...])
```

**Arguments** 

-   `key` — The key part of the pair. [String](../../sql-reference/data-types/string.md).
-   `value` — The value part of the pair. [String](../../sql-reference/data-types/string.md), [Integer](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md).

**Returned value**

-  Data structure as `key:value` pairs.

Type: [Map](../../sql-reference/data-types/map.md).

**Examples**

Query:

``` sql
SELECT mapBuild('apple', number, 'banana', number * 2, 'pear', number * 3) AS map FROM numbers(3);
```

Result:

``` text
┌─map─────────────────────────────┐
│ {'apple':0,'banana':0,'pear':0} │
│ {'apple':1,'banana':2,'pear':3} │
│ {'apple':2,'banana':4,'pear':6} │
└─────────────────────────────────┘
```

## mapContains {#mapcontains}

Determines  whether the `map` contains the `key` parameter.

**Syntax**

``` sql
mapContains(map, key)
```

**Parameters** 

-   `map` — Map. [Map](../../sql-reference/data-types/map.md).
-   `key` — Key. `String`.

**Returned value**

-   `1` if `map` contains `key`, `0` if not.

Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

```sql
SET allow_experimental_map_type = 1;

CREATE TABLE test_map (
  dt Date,
  prices Map(Float64)
) ENGINE = MergeTree
ORDER BY dt;

INSERT INTO test_map(dt, prices) VALUES ('2020-01-01', {'banana': 4.2, 'pear': 3.2}), ('2020-01-02', {'apple': 8.3});

SELECT dt, mapContains(prices, 'apple') FROM test_map ORDER BY dt;
```

Result:

```text
┌─────────dt─┬─mapContains(prices, 'apple')─┐
│ 2020-01-01 │                            0 │
│ 2020-01-02 │                            1 │
└────────────┴──────────────────────────────┘
```

## mapKeys {#mapkeys}

Returns all keys from the `map` parameter.

**Syntax**

```sql
mapKeys(map)
```

**Parameters**

-   `map` — Map. [Map](../../sql-reference/data-types/map.md).

**Returned value**

-   Array containing all keys from the `map`.

Type: [Array](../../sql-reference/data-types/array.md).

**Example**

Query:

```sql
SET allow_experimental_map_type = 1;

CREATE TABLE test_map (
  dt Date,
  prices Map(Float64)
) ENGINE = MergeTree
ORDER BY dt;

INSERT INTO test_map(dt, prices) VALUES ('2020-01-01', {'banana': 4.2, 'pear': 3.2}), ('2020-01-02', {'apple': 8.3});

SELECT dt, mapKeys(prices) FROM test_map ORDER BY dt;
```

Result:

```text
┌─────────dt─┬─mapKeys(prices)───┐
│ 2020-01-01 │ ['banana','pear'] │
│ 2020-01-02 │ ['apple']         │
└────────────┴───────────────────┘
```

## mapValues {#mapvalues}

Returns all values from the `map` parameter.

**Syntax**

```sql
mapKeys(map)
```

**Parameters**

-   `map` — Map. [Map](../../sql-reference/data-types/map.md).

**Returned value**

-   Array containing all the values from `map`.

Type: [Array](../../sql-reference/data-types/array.md).

**Example**

Query:

```sql
SET allow_experimental_map_type = 1;

CREATE TABLE test_map (
  dt Date,
  prices Map(Float64)
) ENGINE = MergeTree
ORDER BY dt;

INSERT INTO test_map(dt, prices) VALUES ('2020-01-01', {'banana': 4.2, 'pear': 3.2}), ('2020-01-02', {'apple': 8.3});

SELECT dt, mapValues(prices) FROM test_map ORDER BY dt;
```

Result:

```text
┌─────────dt─┬─mapValues(prices)─┐
│ 2020-01-01 │ [4.2,3.2]         │
│ 2020-01-02 │ [8.3]             │
└────────────┴───────────────────┘
```

[Original article](https://clickhouse.tech/docs/en/sql-reference/functions/tuple-map-functions/) <!--hide-->
