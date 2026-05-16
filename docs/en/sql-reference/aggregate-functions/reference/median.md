---
description: 'The `median*` functions are the aliases for the corresponding `quantile*`
  functions. They calculate median of a numeric data sample.'
slug: /sql-reference/aggregate-functions/reference/median
title: 'median'
doc_type: 'reference'
---


The `median*` functions are the aliases for the corresponding `quantile*` functions. They calculate median of a numeric data sample.

Functions:

- `median` — Alias for [quantile](/sql-reference/aggregate-functions/reference/quantile).
- `medianDeterministic` — Alias for [quantileDeterministic](/sql-reference/aggregate-functions/reference/quantileDeterministic.md).
- `medianExact` — Alias for [quantileExact](/sql-reference/aggregate-functions/reference/quantileExact.md).
- `medianExactWeighted` — Alias for [quantileExactWeighted](/sql-reference/aggregate-functions/reference/quantileExactWeighted.md).
- `medianTiming` — Alias for [quantileTiming](/sql-reference/aggregate-functions/reference/quantileTiming.md).
- `medianTimingWeighted` — Alias for [quantileTimingWeighted](/sql-reference/aggregate-functions/reference/quantileTimingWeighted.md).
- `medianTDigest` — Alias for [quantileTDigest](/sql-reference/aggregate-functions/reference/quantileTDigest.md).
- `medianTDigestWeighted` — Alias for [quantileTDigestWeighted](/sql-reference/aggregate-functions/reference/quantileTDigestWeighted.md).
- `medianBFloat16` — Alias for [quantileBFloat16](/sql-reference/aggregate-functions/reference/quantileBFloat16.md).
- `medianDD` — Alias for [quantileDD](/sql-reference/aggregate-functions/reference/quantileDD.md).

**Example**

Input table:

```text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

Query:

```sql
SELECT medianDeterministic(val, 1) FROM t;
```

Result:

```text
┌─medianDeterministic(val, 1)─┐
│                         1.5 │
└─────────────────────────────┘
```
