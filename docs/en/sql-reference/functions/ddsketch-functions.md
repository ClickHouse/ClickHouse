---
slug: /en/sql-reference/functions/ddsketch-functions
sidebar_position: 69
sidebar_label: Working with ddsketchs
title: "Functions for ddsketchs"
---


## ddsketchBuild

Build an object with [DDSketch(Float32)](../../sql-reference/data-types/map.md) data type.

**Syntax**

```sql
ddsketchBuild(relative_error, columnName)
```

**Arguments**

-   `relative_error` — a Float32 type value which must be between 0 and 1.
-   `columnName` — a numeric type column.

**Returned value**

-   Data structure represent a DDSketch object.

**Examples**

```sql
WITH thetime = toDateTime64('2022-01-01 10:20:30.999', 3);
INSERT INTO TestSketch
    SELECT 'sendLatency', thetime, ddsketch(0.01, sendLatency) FROM NetTrafficMonitor WHERE timestamp < thetime;
```


## ddsketchQuery

Query [quantile](https://en.wikipedia.org/wiki/Quantile) values based on a specified [percentile rank](https://en.wikipedia.org/wiki/Percentile_rank).

**Syntax**

```sql
ddsketchQuery(sketch, percentile)
```

**Arguments**

-   `sketch` — a DDSketch object
-   `percentile` — a Float32 type value which must be between 0 and 1. Specify percentile with value 0 will return the samllest element in the sketch and 1 will return the biggest element in the sketch.

**Returned value**

-   a Float64 value which represent the original element added to the sketch.

**Example**

```sql
SELECT ddsketchQuery(sketch, 0.5) From TestSketch Where name = 'sendLatency';
```


## ddsketchCount

Get the total element count involved to build the sketch.

**Syntax**

```sql
ddsketchCount(sketch)
```

**Arguments**

-   `sketch` — a DDSketch object

**Returned value**

-   An integer represent the total element count involved to build the sketch.

**Example**


```sql
Select ddsketchCount(sketch) From TestSketch Where name = 'sendLatency';
```


## ddsketchAverage

Returns the average of elements involved to build the sketch.

**Syntax**

```sql
ddsketchAverage(sketch)
```

**Arguments**

-   `sketch` — a DDSketch object

**Returned value**

-   The average of elements involved to build the sketch.

**Example**

```sql
Select ddsketchAverage(sketch) From TestSketch Where name = 'sendLatency';
```


## ddsketchAddSingle

Add one (or multiple element but with same value) element to the sketch

**Syntax**

```sql
ddsketchAddSingle(sketch, element, multiplier)
```

**Arguments**

-   `sketch` — a DDSketch object
-   `element` — the element to be added to the sketch
-   `multiplier` — add same value to the sketch with multiplier times

**Returned value**

-   Data structure represent a DDSketch object.

**Example**

```sql
UPDATE sketch = ddsketchAddSingle(sketch, 0.123, 100) From TestSketch
    WHERE name = 'sendLatency';
```


## ddsketchAddMulti

Add array of elements to the sketch, with corresponding array of multiplier specified.

**Syntax**

```sql
ddsketchAddMulti(sketch, arrayA, arrayB)
```

**Arguments**

-   `sketch` — a DDSketch object
-   `arrayA` — array of element to be added to the sketch
-   `arrayB` — array of multiplier

**Returned value**

-   Data structure represent a DDSketch object.

**Example**

```sql
WITH tmp AS (SELECT count() AS a, sendLatency AS b FROM NetTrafficMonitor WHERE timestamp >= now() group by sendLatency),
tmp1 AS (SELECT groupArray(a) AS arrayA, groupArray(b) AS arrayB FROM tmp)
UPDATE sketch = ddsketchAddSingle(sketch, tmp1.arrayA, tmp1.arrayB) FROM TestSketch WHERE name = 'sendLatency';
```


## ddsketchMerge

Aggregate function that merge a group of sketch objects.

**Syntax**

```sql
ddsketchMerge(sketch)
```

**Arguments**

-   `sketch` — group of DDSketch objects.

**Returned value**

-   Data structure represent a DDSketch object.

**Example**

```sql
SELECT ddsketchQuery(ddsketchMerge(sketch), 0.95) FROM TestSketch 
    WHERE timestamp BETWEEN toDateTime64('2022-01-01 10:20:30.999', 3) AND toDateTime64('2023-01-01 10:20:30.999', 3);
```