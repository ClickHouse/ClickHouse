---
slug: /en/sql-reference/aggregate-functions/reference/contingency
sidebar_position: 116
---

# contingency

The `contingency` function calculates the [contingency coefficient](https://en.wikipedia.org/wiki/Contingency_table#Cram%C3%A9r's_V_and_the_contingency_coefficient_C), a value that measures the association between two columns in a table. The computation is similar to [the `cramersV` function](./cramersv.md) but with a different denominator in the square root.


**Syntax**

``` sql
contingency(column1, column2)
```

**Arguments**

- `column1` and `column2` are the columns to be compared

**Returned value**

- a value between 0 and 1. The larger the result, the closer the association of the two columns.

**Return type** is always [Float64](../../../sql-reference/data-types/float.md).

**Example**

The two columns being compared below have a small association with each other. We have included the result of `cramersV` also (as a comparison):

``` sql
SELECT
    cramersV(a, b),
    contingency(a ,b)
FROM
    (
        SELECT
            number % 10 AS a,
            number % 4 AS b
        FROM
            numbers(150)
    );
```

Result:

```response
┌──────cramersV(a, b)─┬───contingency(a, b)─┐
│ 0.41171788506213564 │ 0.05812725261759165 │
└─────────────────────┴─────────────────────┘
```
