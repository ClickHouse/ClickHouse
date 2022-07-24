---
toc_priority: 69
toc_title: Statistics
---

# Functions for Working with Statistics {#functions-for-working-with-statistics}

# proportionsZTest {#proportionsztest}

Applies proportion z-test to samples from two populations (X and Y). The alternative is 'two-sided'.

**Syntax**

``` sql
proportionsZTest(successes_x, successes_y, trials_x, trials_y, significance_level, usevar)
```

**Arguments**

- `successes_x` — The number of successes for X in trials.
- `successes_y` — The number of successes for X in trials.
- `trials_x` — The number of trials for X.
- `trials_y` — The number of trials for Y.
- `significance_level`
- `usevar` - It can be `'pooled'` or `'unpooled'`.
  - `'pooled'` - The variance of the two populations are assumed to be equal.
  - `'unpooled'` - The assumption of equal variances is dropped.

**Returned value**

-   A tuple with the (z-statistic, p-value, confidence-interval-lower, confidence-interval-upper).

Type: [Tuple](../../sql-reference/data-types/tuple.md).

**Example**

Query:

``` sql
SELECT proportionsZTest(10, 11, 100, 101, 0.95, 'unpooled');
```

Result:

``` text
(-0.20656724435948853,0.8363478437079654,-0.09345975390115283,0.07563797172293502)
```

