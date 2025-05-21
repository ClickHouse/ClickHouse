---
slug: /zh/sql-reference/aggregate-functions/reference/median
---
# median {#median}

`median*` 函数是 `quantile*` 函数的别名。它们计算数字数据样本的中位数。

函数:

-   `median` — [quantile](/docs/zh/sql-reference/aggregate-functions/reference/quantile)别名。
-   `medianDeterministic` — [quantileDeterministic](/docs/zh/sql-reference/aggregate-functions/reference/quantiledeterministic)别名。
-   `medianExact` — [quantileExact](/docs/zh/sql-reference/aggregate-functions/reference/quantileexact)别名。
-   `medianExactWeighted` — [quantileExactWeighted](/docs/zh/sql-reference/aggregate-functions/reference/quantileexactweighted)别名。
-   `medianTiming` — [quantileTiming](/docs/zh/sql-reference/aggregate-functions/reference/quantiletiming)别名。
-   `medianTimingWeighted` — [quantileTimingWeighted](/docs/zh/sql-reference/aggregate-functions/reference/quantiletimingweighted)别名。
-   `medianTDigest` — [quantileTDigest](/docs/zh/sql-reference/aggregate-functions/reference/quantiletdigest)别名。
-   `medianTDigestWeighted` — [quantileTDigestWeighted](/docs/zh/sql-reference/aggregate-functions/reference/quantiletdigestweighted)别名。

**示例**

输入表:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

查询:

``` sql
SELECT medianDeterministic(val, 1) FROM t
```

结果:

``` text
┌─medianDeterministic(val, 1)─┐
│                         1.5 │
└─────────────────────────────┘
```
