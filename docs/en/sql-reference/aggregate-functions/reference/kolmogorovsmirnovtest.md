---
slug: /en/sql-reference/aggregate-functions/reference/kolmogorovsmirnovtest
sidebar_position: 156
sidebar_label: kolmogorovSmirnovTest
---

# kolmogorovSmirnovTest

Applies Kolmogorov-Smirnov's test to samples from two populations.

**Syntax**

``` sql
kolmogorovSmirnovTest([alternative, computation_method])(sample_data, sample_index)
```

Values of both samples are in the `sample_data` column. If `sample_index` equals to 0 then the value in that row belongs to the sample from the first population. Otherwise it belongs to the sample from the second population.
Samples must belong to continuous, one-dimensional probability distributions.

**Arguments**

- `sample_data` — Sample data. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) or [Decimal](../../../sql-reference/data-types/decimal.md).
- `sample_index` — Sample index. [Integer](../../../sql-reference/data-types/int-uint.md).

**Parameters**

- `alternative` — alternative hypothesis. (Optional, default: `'two-sided'`.) [String](../../../sql-reference/data-types/string.md).
    Let F(x) and G(x) be the CDFs of the first and second distributions respectively.
    - `'two-sided'`
        The null hypothesis is that samples come from the same distribution, e.g. F(x) = G(x) for all x.
        And the alternative is that the distributions are not identical.
    - `'greater'`
        The null hypothesis is that values in the first sample are *stochastically smaller* than those in the second one,
        e.g. the CDF of first distribution lies above and hence to the left of that for the second one.
        Which in fact means that F(x) >= G(x) for all x. And the alternative in this case is that F(x) < G(x) for at least one x.
    - `'less'`.
        The null hypothesis is that values in the first sample are *stochastically greater* than those in the second one,
        e.g. the CDF of first distribution lies below and hence to the right of that for the second one.
        Which in fact means that F(x) <= G(x) for all x. And the alternative in this case is that F(x) > G(x) for at least one x.
- `computation_method` — the method used to compute p-value. (Optional, default: `'auto'`.) [String](../../../sql-reference/data-types/string.md).
    - `'exact'` - calculation is performed using precise probability distribution of the test statistics. Compute intensive and wasteful except for small samples.
    - `'asymp'` (`'asymptotic'`) - calculation is performed using an approximation. For large sample sizes, the exact and asymptotic p-values are very similar.
    - `'auto'`  - the `'exact'` method is used when a maximum number of samples is less than 10'000.


**Returned values**

[Tuple](../../../sql-reference/data-types/tuple.md) with two elements:

- calculated statistic. [Float64](../../../sql-reference/data-types/float.md).
- calculated p-value. [Float64](../../../sql-reference/data-types/float.md).


**Example**

Query:

``` sql
SELECT kolmogorovSmirnovTest('less', 'exact')(value, num)
FROM
(
    SELECT
        randNormal(0, 10) AS value,
        0 AS num
    FROM numbers(10000)
    UNION ALL
    SELECT
        randNormal(0, 10) AS value,
        1 AS num
    FROM numbers(10000)
)
```

Result:

``` text
┌─kolmogorovSmirnovTest('less', 'exact')(value, num)─┐
│ (0.009899999999999996,0.37528595205132287)         │
└────────────────────────────────────────────────────┘
```

Note:
P-value is bigger than 0.05 (for confidence level of 95%), so null hypothesis is not rejected.


Query:

``` sql
SELECT kolmogorovSmirnovTest('two-sided', 'exact')(value, num)
FROM
(
    SELECT
        randStudentT(10) AS value,
        0 AS num
    FROM numbers(100)
    UNION ALL
    SELECT
        randNormal(0, 10) AS value,
        1 AS num
    FROM numbers(100)
)
```

Result:

``` text
┌─kolmogorovSmirnovTest('two-sided', 'exact')(value, num)─┐
│ (0.4100000000000002,6.61735760482795e-8)                │
└─────────────────────────────────────────────────────────┘
```

Note:
P-value is less than 0.05 (for confidence level of 95%), so null hypothesis is rejected.


**See Also**

- [Kolmogorov-Smirnov'test](https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test)
