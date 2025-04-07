---
slug: /en/sql-reference/aggregate-functions/reference/sumcount
sidebar_position: 196
title: sumCount
---

Calculates the sum of the numbers and counts the number of rows at the same time. The function is used by ClickHouse query optimizer: if there are multiple `sum`, `count` or `avg` functions in a query, they can be replaced to single `sumCount` function to reuse the calculations. The function is rarely needed to use explicitly.

**Syntax**

``` sql
sumCount(x)
```

**Arguments**

- `x` — Input value, must be [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), or [Decimal](../../../sql-reference/data-types/decimal.md).

**Returned value**

- Tuple `(sum, count)`, where `sum` is the sum of numbers and `count` is the number of rows with not-NULL values.

Type: [Tuple](../../../sql-reference/data-types/tuple.md).

**Example**

Query:

``` sql
CREATE TABLE s_table (x Int8) Engine = Log;
INSERT INTO s_table SELECT number FROM numbers(0, 20);
INSERT INTO s_table VALUES (NULL);
SELECT sumCount(x) from s_table;
```

Result:

``` text
┌─sumCount(x)─┐
│ (190,20)    │
└─────────────┘
```

**See also**

- [optimize_syntax_fuse_functions](../../../operations/settings/settings.md#optimize_syntax_fuse_functions) setting.
