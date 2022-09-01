---
toc_priority: 201
---

# quantiles {#quantiles}

**语法**
``` sql
quantiles(level1, level2, …)(x)
```

所有分位数函数(quantile)也有相应的分位数(quantiles)函数: `quantiles`, `quantilesDeterministic`, `quantilesTiming`, `quantilesTimingWeighted`, `quantilesExact`, `quantilesExactWeighted`, `quantilesTDigest`。 这些函数一次计算所列的级别的所有分位数, 并返回结果值的数组。
