## rankCorr {#agg_function-rankcorr}

计算等级相关系数。

**语法**

``` sql
rankCorr(x, y)
```

**参数**

-   `x` — 任意值。[Float32](../../../sql-reference/data-types/float.md#float32-float64) 或 [Float64](../../../sql-reference/data-types/float.md#float32-float64)。
-   `y` — 任意值。[Float32](../../../sql-reference/data-types/float.md#float32-float64) 或 [Float64](../../../sql-reference/data-types/float.md#float32-float64)。

**返回值**

-   Returns a rank correlation coefficient of the ranks of x and y. The value of the correlation coefficient ranges from -1 to +1. If less than two arguments are passed, the function will return an exception. The value close to +1 denotes a high linear relationship, and with an increase of one random variable, the second random variable also increases. The value close to -1 denotes a high linear relationship, and with an increase of one random variable, the second random variable decreases. The value close or equal to 0 denotes no relationship between the two random variables.

类型: [Float64](../../../sql-reference/data-types/float.md#float32-float64)。  

**示例**

查询:

``` sql
SELECT rankCorr(number, number) FROM numbers(100);
```

结果:

``` text
┌─rankCorr(number, number)─┐
│                        1 │
└──────────────────────────┘
```

查询:

``` sql
SELECT roundBankers(rankCorr(exp(number), sin(number)), 3) FROM numbers(100);
```

结果:

``` text
┌─roundBankers(rankCorr(exp(number), sin(number)), 3)─┐
│                                              -0.037 │
└─────────────────────────────────────────────────────┘
```
**参见**

-   斯皮尔曼等级相关系数[Spearman's rank correlation coefficient](https://en.wikipedia.org/wiki/Spearman%27s_rank_correlation_coefficient)