---
slug: /en/sql-reference/aggregate-functions/reference/boundingRatio
sidebar_position: 114
title: boundingRatio
---

Aggregate function that calculates the slope between the leftmost and rightmost points across a group of values.

Example:

Sample data:
```sql
SELECT
    number,
    number * 1.5
FROM numbers(10)
```
```response
┌─number─┬─multiply(number, 1.5)─┐
│      0 │                     0 │
│      1 │                   1.5 │
│      2 │                     3 │
│      3 │                   4.5 │
│      4 │                     6 │
│      5 │                   7.5 │
│      6 │                     9 │
│      7 │                  10.5 │
│      8 │                    12 │
│      9 │                  13.5 │
└────────┴───────────────────────┘
```

The boundingRatio() function returns the slope of the line between the leftmost and rightmost points, in the above data these points are `(0,0)` and `(9,13.5)`.

```sql
SELECT boundingRatio(number, number * 1.5)
FROM numbers(10)
```
```response
┌─boundingRatio(number, multiply(number, 1.5))─┐
│                                          1.5 │
└──────────────────────────────────────────────┘
```

