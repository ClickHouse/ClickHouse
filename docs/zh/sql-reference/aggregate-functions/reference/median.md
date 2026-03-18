---
slug: /zh/sql-reference/aggregate-functions/reference/median
---
# median {#median}

`median*` 函数是 `quantile*` 函数的别名。它们计算数字数据样本的中位数。

函数:

-   `median` — [quantile](/zh/sql-reference/aggregate-functions/reference/quantile)别名。
-   `medianDeterministic` — [quantileDeterministic](/zh/sql-reference/aggregate-functions/reference/quantiledeterministic)别名。
-   `medianExact` — [quantileExact](/zh/sql-reference/aggregate-functions/reference/quantileexact)别名。
-   `medianExactWeighted` — [quantileExactWeighted](/zh/sql-reference/aggregate-functions/reference/quantileexactweighted)别名。
-   `medianTiming` — [quantileTiming](/zh/sql-reference/aggregate-functions/reference/quantiletiming)别名。
-   `medianTimingWeighted` — [quantileTimingWeighted](/zh/sql-reference/aggregate-functions/reference/quantiletimingweighted)别名。
-   `medianTDigest` — [quantileTDigest](/zh/sql-reference/aggregate-functions/reference/quantiletdigest)别名。
-   `medianTDigestWeighted` — [quantileTDigestWeighted](/zh/sql-reference/aggregate-functions/reference/quantiletdigestweighted)别名。

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
