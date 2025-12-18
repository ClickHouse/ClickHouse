---
description: 'Applies the one-sample Student t-test to a sample and a known population mean.'
sidebar_label: 'studentTTestOneSample'
sidebar_position: 195
slug: /sql-reference/aggregate-functions/reference/studentttestonesample
title: 'studentTTestOneSample'
doc_type: 'reference'
---

# studentTTestOneSample

Applies the one-sample Student's t-test to determine whether the mean of a sample differs from a known population mean.

Normality is assumed. The null hypothesis is that the sample mean equals the population mean.

**Syntax**

```sql
studentTTestOneSample([confidence_level])(sample_data, population_mean)
```

The optional `confidence_level` enables confidence interval calculation.

**Arguments**

- `sample_data` — Sample data. Integer, Float or Decimal.
- `population_mean` — Known population mean to test against. Integer, Float or Decimal (usually a constant).

**Parameters**

- `confidence_level` — Confidence level for confidence intervals. Float in (0, 1).

Notes:
- At least 2 observations are required; otherwise the result is `(nan, nan)` (and intervals if requested are `nan`).
- Constant or near-constant input will also return `nan` due to zero (or effectively zero) standard error.

**Returned values**

[Tuple](../../../sql-reference/data-types/tuple.md) with two or four elements (if `confidence_level` is specified):

- calculated t-statistic. Float64.
- calculated p-value (two-tailed). Float64.
- calculated confidence-interval-low. Float64. (optional)
- calculated confidence-interval-high. Float64. (optional)

Confidence intervals are for the sample mean at the given confidence level.

**Examples**

Input table:

```text
┌─value─┐
│  20.3 │
│  21.1 │
│  21.7 │
│  19.9 │
│  21.8 │
└───────┘
```

Without confidence interval:

```sql
SELECT studentTTestOneSample()(value, 20.0) FROM t;
-- or simply
SELECT studentTTestOneSample(value, 20.0) FROM t;
```

With confidence interval (95%):

```sql
SELECT studentTTestOneSample(0.95)(value, 20.0) FROM t;
```

**See Also**

- [Student's t-test](https://en.wikipedia.org/wiki/Student%27s_t-test)
- [studentTTest function](/sql-reference/aggregate-functions/reference/studentttest)
