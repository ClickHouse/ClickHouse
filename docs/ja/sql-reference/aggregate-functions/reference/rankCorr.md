---
slug: /ja/sql-reference/aggregate-functions/reference/rankCorr
sidebar_position: 182
---

# rankCorr

順位相関係数を計算します。

**構文**

``` sql
rankCorr(x, y)
```

**引数**

- `x` — 任意の値。[Float32](../../../sql-reference/data-types/float.md#float32-float64) または [Float64](../../../sql-reference/data-types/float.md#float32-float64)。
- `y` — 任意の値。[Float32](../../../sql-reference/data-types/float.md#float32-float64) または [Float64](../../../sql-reference/data-types/float.md#float32-float64)。

**返される値**

- `x` と `y` のランクの順位相関係数を返します。相関係数の値は -1 から +1 の範囲です。2つ以上の引数が渡されない場合、関数は例外を返します。+1 に近い値は高い線形関係を示し、一つのランダム変数が増加するともう一つのランダム変数も増加します。-1 に近い値は高い線形関係を示し、一つのランダム変数が増加するともう一つのランダム変数が減少します。0 に近いまたは 0 の値は、2つのランダム変数間に関係がないことを示します。

型: [Float64](../../../sql-reference/data-types/float.md#float32-float64)。

**例**

クエリ:

``` sql
SELECT rankCorr(number, number) FROM numbers(100);
```

結果:

``` text
┌─rankCorr(number, number)─┐
│                        1 │
└──────────────────────────┘
```

クエリ:

``` sql
SELECT roundBankers(rankCorr(exp(number), sin(number)), 3) FROM numbers(100);
```

結果:

``` text
┌─roundBankers(rankCorr(exp(number), sin(number)), 3)─┐
│                                              -0.037 │
└─────────────────────────────────────────────────────┘
```

**参照**

- [スピアマンの順位相関係数](https://en.wikipedia.org/wiki/Spearman%27s_rank_correlation_coefficient)
