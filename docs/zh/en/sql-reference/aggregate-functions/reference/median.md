---
description: '`median*` 函数是相应 `quantile*`
  函数的别名。它们用于计算数值数据样本的中位数。'
slug: /sql-reference/aggregate-functions/reference/median
title: 'median'
doc_type: 'reference'
---

`median*` 函数是相应 `quantile*` 函数的别名。它们用于计算数值数据样本的中位数。

函数：

* `median` — [quantile](/zh/sql-reference/aggregate-functions/reference/quantile) 的别名。
* `medianDeterministic` — [quantileDeterministic](/zh/sql-reference/aggregate-functions/reference/quantileDeterministic.md) 的别名。
* `medianExact` — [quantileExact](/zh/sql-reference/aggregate-functions/reference/quantileExact.md) 的别名。
* `medianExactWeighted` — [quantileExactWeighted](/zh/sql-reference/aggregate-functions/reference/quantileExactWeighted.md) 的别名。
* `medianTiming` — [quantileTiming](/zh/sql-reference/aggregate-functions/reference/quantileTiming.md) 的别名。
* `medianTimingWeighted` — [quantileTimingWeighted](/zh/sql-reference/aggregate-functions/reference/quantileTimingWeighted.md) 的别名。
* `medianTDigest` — [quantileTDigest](/zh/sql-reference/aggregate-functions/reference/quantileTDigest.md) 的别名。
* `medianTDigestWeighted` — [quantileTDigestWeighted](/zh/sql-reference/aggregate-functions/reference/quantileTDigestWeighted.md) 的别名。
* `medianBFloat16` — [quantileBFloat16](/zh/sql-reference/aggregate-functions/reference/quantileBFloat16.md) 的别名。
* `medianDD` — [quantileDD](/zh/sql-reference/aggregate-functions/reference/quantileDD.md) 的别名。

**示例**

输入表：

```text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

```sql title="Query"
SELECT medianDeterministic(val, 1) FROM t;
```

```text title="Response"
┌─medianDeterministic(val, 1)─┐
│                         1.5 │
└─────────────────────────────┘
```