---
slug: /en/sql-reference/aggregate-functions/reference/theilsu
sidebar_position: 201
---

# theilsU

The `theilsU` function calculates the [Theil's U uncertainty coefficient](https://en.wikipedia.org/wiki/Contingency_table#Uncertainty_coefficient), a value that measures the association between two columns in a table. Its values range from −1.0 (100% negative association, or perfect inversion) to +1.0 (100% positive association, or perfect agreement). A value of 0.0 indicates the absence of association.


**Syntax**

``` sql
theilsU(column1, column2)
```

**Arguments**

- `column1` and `column2` are the columns to be compared

**Returned value**

- a value between -1 and 1

**Return type** is always [Float64](../../../sql-reference/data-types/float.md).

**Example**

The following two columns being compared below have a small association with each other, so the value of `theilsU` is negative:

``` sql
SELECT
    theilsU(a ,b)
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
┌────────theilsU(a, b)─┐
│ -0.30195720557678846 │
└──────────────────────┘
```
