---
slug: /en/sql-reference/aggregate-functions/reference/cramersvbiascorrected
sidebar_position: 128
---

# cramersVBiasCorrected

Cramer's V is a measure of association between two columns in a table. The result of the [`cramersV` function](./cramersv.md) ranges from 0 (corresponding to no association between the variables) to 1 and can reach 1 only when each value is completely determined by the other. The function can be heavily biased, so this version of Cramer's V uses the [bias correction](https://en.wikipedia.org/wiki/Cram%C3%A9r%27s_V#Bias_correction).

**Syntax**

``` sql
cramersVBiasCorrected(column1, column2)
```

**Parameters**

- `column1`: first column to be compared.
- `column2`: second column to be compared.

**Returned value**

- a value between 0 (corresponding to no association between the columns' values) to 1 (complete association).

Type: always [Float64](../../../sql-reference/data-types/float.md).

**Example**

The following two columns being compared below have a small association with each other. Notice the result of `cramersVBiasCorrected` is smaller than the result of `cramersV`:

Query:

``` sql
SELECT
    cramersV(a, b),
    cramersVBiasCorrected(a ,b)
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
┌──────cramersV(a, b)─┬─cramersVBiasCorrected(a, b)─┐
│ 0.41171788506213564 │         0.33369281784141364 │
└─────────────────────┴─────────────────────────────┘
```
