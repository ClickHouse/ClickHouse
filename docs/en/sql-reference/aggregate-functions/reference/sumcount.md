---
toc_priority: 144
---

# sumCount {#agg_function-sumCount}

Calculates the sum of the numbers and counts the number of rows at the same time.

**Syntax**

``` sql
sumCount(x)
```

**Arguments** 

-   `x` — Input value, must be [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), or [Decimal](../../../sql-reference/data-types/decimal.md).

**Returned value**

-   Tuple `(sum, count)`, where `sum` is the sum of numbers and `count` is the number of rows with not-NULL values.

Type: [Tuple](../../../sql-reference/data-types/tuple.md).

**Example**

Query:

``` sql
CREATE TABLE test (x Int8) Engine = Log;
INSERT INTO test SELECT number FROM numbers(1, 20);
SELECT sumCount(x) from test;
```

Result:

``` text
┌─sumCount(a)─┐
│ (210,20)    │
└─────────────┘
```

**See also**

- [optimize_fuse_sum_count_avg](../../../operations/settings/settings.md#optimize_fuse_sum_count_avg) setting.
