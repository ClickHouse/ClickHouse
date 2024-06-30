---
slug: /en/sql-reference/aggregate-functions/reference/cramersv
sidebar_position: 127
---

# cramersV

[Cramer's V](https://en.wikipedia.org/wiki/Cram%C3%A9r%27s_V) (sometimes referred to as Cramer's phi) is a measure of association between two columns in a table. The result of the `cramersV` function ranges from 0 (corresponding to no association between the variables) to 1 and can reach 1 only when each value is completely determined by the other. It may be viewed as the association between two variables as a percentage of their maximum possible variation.

:::note
For a bias corrected version of Cramer's V see: [cramersVBiasCorrected](./cramersvbiascorrected.md)
:::

**Syntax**

``` sql
cramersV(column1, column2)
```

**Parameters**

- `column1`: first column to be compared.
- `column2`: second column to be compared.

**Returned value**

- a value between 0 (corresponding to no association between the columns' values) to 1 (complete association).

Type: always [Float64](../../../sql-reference/data-types/float.md).

**Example**

The following two columns being compared below have no association with each other, so the result of `cramersV` is 0:

Query:

``` sql
SELECT
    cramersV(a, b)
FROM
    (
        SELECT
            number % 3 AS a,
            number % 5 AS b
        FROM
            numbers(150)
    );
```

Result:

```response
┌─cramersV(a, b)─┐
│              0 │
└────────────────┘
```

The following two columns below have a fairly close association, so the result of `cramersV` is a high value:

```sql
SELECT
    cramersV(a, b)
FROM
    (
        SELECT
            number % 10 AS a,
            number % 5 AS b
        FROM
            numbers(150)
    );
```

Result:

```response
┌─────cramersV(a, b)─┐
│ 0.8944271909999159 │
└────────────────────┘
```
