# median {#median}

`median*` 函数是 `quantile*` 函数的别名。它们计算数字数据样本的中位数。

函数:

-   `median` — [quantile](#quantile)别名。
-   `medianDeterministic` — [quantileDeterministic](#quantiledeterministic)别名。
-   `medianExact` — [quantileExact](#quantileexact)别名。
-   `medianExactWeighted` — [quantileExactWeighted](#quantileexactweighted)别名。
-   `medianTiming` — [quantileTiming](#quantiletiming)别名。
-   `medianTimingWeighted` — [quantileTimingWeighted](#quantiletimingweighted)别名。
-   `medianTDigest` — [quantileTDigest](#quantiletdigest)别名。
-   `medianTDigestWeighted` — [quantileTDigestWeighted](#quantiletdigestweighted)别名。

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
