---
slug: /ja/sql-reference/aggregate-functions/reference/quantiles
sidebar_position: 177
---

# quantiles 関数

## quantiles

構文: `quantiles(level1, level2, ...)(x)`

すべての分位関数には、対応する分位関数もあります: `quantiles`, `quantilesDeterministic`, `quantilesTiming`, `quantilesTimingWeighted`, `quantilesExact`, `quantilesExactWeighted`, `quantileExactWeightedInterpolated`, `quantileInterpolatedWeighted`, `quantilesTDigest`, `quantilesBFloat16`, `quantilesDD`。これらの関数は、指定されたレベルのすべての分位数を一度で計算し、結果の値の配列を返します。

## quantilesExactExclusive

数値データシーケンスの[分位数](https://en.wikipedia.org/wiki/Quantile)を正確に計算します。

正確な値を取得するために、すべての渡された値を配列に結合し、その後一部をソートします。そのため、この関数は渡された値の数 `n` に比例して `O(n)` のメモリを消費します。ただし、値の数が少ない場合、この関数は非常に効果的です。

この関数は Excel の [PERCENTILE.EXC](https://support.microsoft.com/en-us/office/percentile-exc-function-bbaa7204-e9e1-4010-85bf-c31dc5dce4ba) 関数に相当し、([型 R6](https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample)) に相当します。

[quantileExactExclusive](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexactexclusive) よりもセットレベルで効率的に動作します。

**構文**

``` sql
quantilesExactExclusive(level1, level2, ...)(expr)
```

**引数**

- `expr` — 数値 [データ型](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) または [DateTime](../../../sql-reference/data-types/datetime.md) 結果をもたらすカラム値に対する式。

**パラメータ**

- `level` — 分位数のレベル。可能な値: (0, 1) — 境界を含まない。 [Float](../../../sql-reference/data-types/float.md)。

**返される値**

- 指定されたレベルの分位数の [Array](../../../sql-reference/data-types/array.md)。

配列値の型:

- 数値データ型入力の場合は [Float64](../../../sql-reference/data-types/float.md)。
- 入力値が `Date` 型の場合は [Date](../../../sql-reference/data-types/date.md)。
- 入力値が `DateTime` 型の場合は [DateTime](../../../sql-reference/data-types/datetime.md)。

**例**

クエリ:

``` sql
CREATE TABLE num AS numbers(1000);

SELECT quantilesExactExclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x) FROM (SELECT number AS x FROM num);
```

結果:

``` text
┌─quantilesExactExclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x)─┐
│ [249.25,499.5,749.75,899.9,949.9499999999999,989.99,998.999]        │
└─────────────────────────────────────────────────────────────────────┘
```

## quantilesExactInclusive

数値データシーケンスの[分位数](https://en.wikipedia.org/wiki/Quantile)を正確に計算します。

正確な値を取得するために、すべての渡された値を配列に結合し、その後一部をソートします。そのため、この関数は渡された値の数 `n` に比例して `O(n)` のメモリを消費します。ただし、値の数が少ない場合、この関数は非常に効果的です。

この関数は Excel の [PERCENTILE.INC](https://support.microsoft.com/en-us/office/percentile-inc-function-680f9539-45eb-410b-9a5e-c1355e5fe2ed) 関数に相当し、([型 R7](https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample)) に相当します。

[quantileExactInclusive](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexactinclusive) よりもセットレベルで効率的に動作します。

**構文**

``` sql
quantilesExactInclusive(level1, level2, ...)(expr)
```

**引数**

- `expr` — 数値 [データ型](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) または [DateTime](../../../sql-reference/data-types/datetime.md) 結果をもたらすカラム値に対する式。

**パラメータ**

- `level` — 分位数のレベル。可能な値: [0, 1] — 境界を含む。 [Float](../../../sql-reference/data-types/float.md)。

**返される値**

- 指定されたレベルの分位数の [Array](../../../sql-reference/data-types/array.md)。

配列値の型:

- 数値データ型入力の場合は [Float64](../../../sql-reference/data-types/float.md)。
- 入力値が `Date` 型の場合は [Date](../../../sql-reference/data-types/date.md)。
- 入力値が `DateTime` 型の場合は [DateTime](../../../sql-reference/data-types/datetime.md)。

**例**

クエリ:

``` sql
CREATE TABLE num AS numbers(1000);

SELECT quantilesExactInclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x) FROM (SELECT number AS x FROM num);
```

結果:

``` text
┌─quantilesExactInclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x)─┐
│ [249.75,499.5,749.25,899.1,949.05,989.01,998.001]                   │
└─────────────────────────────────────────────────────────────────────┘
```

## quantilesGK

`quantilesGK` は `quantileGK` に似た動作をし、異なるレベルでの分位数を同時に計算し、配列を返します。

**構文**

``` sql
quantilesGK(accuracy, level1, level2, ...)(expr)
```

**返される値**

- 指定されたレベルの分位数の [Array](../../../sql-reference/data-types/array.md)。

配列値の型:

- 数値データ型入力の場合は [Float64](../../../sql-reference/data-types/float.md)。
- 入力値が `Date` 型の場合は [Date](../../../sql-reference/data-types/date.md)。
- 入力値が `DateTime` 型の場合は [DateTime](../../../sql-reference/data-types/datetime.md)。

**例**

クエリ:

``` sql
SELECT quantilesGK(1, 0.25, 0.5, 0.75)(number + 1)
FROM numbers(1000)

┌─quantilesGK(1, 0.25, 0.5, 0.75)(plus(number, 1))─┐
│ [1,1,1]                                          │
└──────────────────────────────────────────────────┘

SELECT quantilesGK(10, 0.25, 0.5, 0.75)(number + 1)
FROM numbers(1000)

┌─quantilesGK(10, 0.25, 0.5, 0.75)(plus(number, 1))─┐
│ [156,413,659]                                     │
└───────────────────────────────────────────────────┘


SELECT quantilesGK(100, 0.25, 0.5, 0.75)(number + 1)
FROM numbers(1000)

┌─quantilesGK(100, 0.25, 0.5, 0.75)(plus(number, 1))─┐
│ [251,498,741]                                      │
└────────────────────────────────────────────────────┘

SELECT quantilesGK(1000, 0.25, 0.5, 0.75)(number + 1)
FROM numbers(1000)

┌─quantilesGK(1000, 0.25, 0.5, 0.75)(plus(number, 1))─┐
│ [249,499,749]                                       │
└─────────────────────────────────────────────────────┘
```
