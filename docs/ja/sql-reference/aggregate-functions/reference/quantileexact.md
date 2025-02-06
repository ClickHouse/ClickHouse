---
slug: /ja/sql-reference/aggregate-functions/reference/quantileexact
sidebar_position: 173
---

# quantileExact 関数

## quantileExact

数値データ列の[分位数](https://en.wikipedia.org/wiki/Quantile)を正確に計算します。

正確な値を取得するために、渡された全ての値は配列に結合され、部分的にソートされます。そのため、関数は `O(n)` のメモリを消費し、ここで `n` は渡された値の数です。ただし、少数の値の場合、この関数は非常に効果的です。

異なるレベルの複数の `quantile*` 関数をクエリで使用する場合、内部状態は結合されません（つまり、クエリは可能な限り効率的に動作しません）。この場合、[quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) 関数を使用してください。

**構文**

``` sql
quantileExact(level)(expr)
```

エイリアス: `medianExact`。

**引数**

- `level` — 分位数のレベル。オプションのパラメーター。0から1までの定数の浮動小数点数。`level` 値を `[0.01, 0.99]` 範囲で使用することをお勧めします。デフォルト値: 0.5。`level=0.5` の場合、関数は[中央値](https://en.wikipedia.org/wiki/Median)を計算します。
- `expr` — 数値型の[データタイプ](../../../sql-reference/data-types/index.md#data_types)、[Date](../../../sql-reference/data-types/date.md)、または[DateTime](../../../sql-reference/data-types/datetime.md)に結果するカラム値に対する式。

**返される値**

- 指定されたレベルの分位数。

タイプ:

- 数値データ型入力の場合は [Float64](../../../sql-reference/data-types/float.md)。
- 入力値が `Date` 型の場合は [Date](../../../sql-reference/data-types/date.md)。
- 入力値が `DateTime` 型の場合は [DateTime](../../../sql-reference/data-types/datetime.md)。

**例**

クエリ:

``` sql
SELECT quantileExact(number) FROM numbers(10)
```

結果:

``` text
┌─quantileExact(number)─┐
│                     5 │
└───────────────────────┘
```

## quantileExactLow

`quantileExact` と同様に、数値データ列の正確な[分位数](https://en.wikipedia.org/wiki/Quantile)を計算します。

正確な値を取得するために、渡された全ての値は配列に結合され、完全にソートされます。ソート[アルゴリズム](https://en.cppreference.com/w/cpp/algorithm/sort)の複雑さは `O(N·log(N))` であり、ここで `N = std::distance(first, last)` 比較があります。

返される値は分位数のレベルと選択内の要素数に依存します。例えば、レベルが0.5の場合、偶数個の要素には下位の中央値を返し、奇数個の要素には中央の中央値を返します。中央値は、pythonで使用される [median_low](https://docs.python.org/3/library/statistics.html#statistics.median_low) 実装と同様に計算されます。

他の全てのレベルでは、`level * size_of_array` の値に対応するインデックスの要素が返されます。例えば:

``` sql
SELECT quantileExactLow(0.1)(number) FROM numbers(10)

┌─quantileExactLow(0.1)(number)─┐
│                             1 │
└───────────────────────────────┘
```

異なるレベルの複数の `quantile*` 関数をクエリで使用する場合、内部状態は結合されません（つまり、クエリは可能な限り効率的に動作しません）。この場合、[quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) 関数を使用してください。

**構文**

``` sql
quantileExactLow(level)(expr)
```

エイリアス: `medianExactLow`。

**引数**

- `level` — 分位数のレベル。オプションのパラメーター。0から1までの定数の浮動小数点数。`level` 値を `[0.01, 0.99]` 範囲で使用することをお勧めします。デフォルト値: 0.5。`level=0.5` の場合、関数は[中央値](https://en.wikipedia.org/wiki/Median)を計算します。
- `expr` — 数値型の[データタイプ](../../../sql-reference/data-types/index.md#data_types)、[Date](../../../sql-reference/data-types/date.md)、または[DateTime](../../../sql-reference/data-types/datetime.md)に結果するカラム値に対する式。

**返される値**

- 指定されたレベルの分位数。

タイプ:

- 数値データ型入力の場合は [Float64](../../../sql-reference/data-types/float.md)。
- 入力値が `Date` 型の場合は [Date](../../../sql-reference/data-types/date.md)。
- 入力値が `DateTime` 型の場合は [DateTime](../../../sql-reference/data-types/datetime.md)。

**例**

クエリ:

``` sql
SELECT quantileExactLow(number) FROM numbers(10)
```

結果:

``` text
┌─quantileExactLow(number)─┐
│                        4 │
└──────────────────────────┘
```
## quantileExactHigh

`quantileExact` と同様に、数値データ列の正確な[分位数](https://en.wikipedia.org/wiki/Quantile)を計算します。

渡された全ての値は配列に結合され、正確な値を取得するために完全にソートされます。ソート[アルゴリズム](https://en.cppreference.com/w/cpp/algorithm/sort)の複雑さは `O(N·log(N))` であり、ここで `N = std::distance(first, last)` 比較があります。

返される値は分位数のレベルと選択内の要素数に依存します。例えば、レベルが0.5の場合、偶数個の要素には上位の中央値を返し、奇数個の要素には中央の中央値を返します。中央値は、pythonで使用される [median_high](https://docs.python.org/3/library/statistics.html#statistics.median_high) 実装と同様に計算されます。他の全てのレベルでは、`level * size_of_array` の値に対応するインデックスの要素が返されます。

この実装は、現在の `quantileExact` 実装と完全に同じように動作します。

異なるレベルの複数の `quantile*` 関数をクエリで使用する場合、内部状態は結合されません（つまり、クエリは可能な限り効率的に動作しません）。この場合、[quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) 関数を使用してください。

**構文**

``` sql
quantileExactHigh(level)(expr)
```

エイリアス: `medianExactHigh`。

**引数**

- `level` — 分位数のレベル。オプションのパラメーター。0から1までの定数の浮動小数点数。`level` 値を `[0.01, 0.99]` 範囲で使用することをお勧めします。デフォルト値: 0.5。`level=0.5` の場合、関数は[中央値](https://en.wikipedia.org/wiki/Median)を計算します。
- `expr` — 数値型の[データタイプ](../../../sql-reference/data-types/index.md#data_types)、[Date](../../../sql-reference/data-types/date.md)、または[DateTime](../../../sql-reference/data-types/datetime.md)に結果するカラム値に対する式。

**返される値**

- 指定されたレベルの分位数。

タイプ:

- 数値データ型入力の場合は [Float64](../../../sql-reference/data-types/float.md)。
- 入力値が `Date` 型の場合は [Date](../../../sql-reference/data-types/date.md)。
- 入力値が `DateTime` 型の場合は [DateTime](../../../sql-reference/data-types/datetime.md)。

**例**

クエリ:

``` sql
SELECT quantileExactHigh(number) FROM numbers(10)
```

結果:

``` text
┌─quantileExactHigh(number)─┐
│                         5 │
└──────────────────────────┘
```

## quantileExactExclusive

数値データ列の[分位数](https://en.wikipedia.org/wiki/Quantile)を正確に計算します。

正確な値を取得するために、渡された全ての値は配列に結合され、部分的にソートされます。そのため、関数は `O(n)` のメモリを消費し、ここで `n` は渡された値の数です。ただし、少数の値の場合、この関数は非常に効果的です。

この関数は、Excel の [PERCENTILE.EXC](https://support.microsoft.com/en-us/office/percentile-exc-function-bbaa7204-e9e1-4010-85bf-c31dc5dce4ba) 関数と同等であり、（[タイプ R6](https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample)）です。

異なるレベルの `quantileExactExclusive` 関数をクエリで使用する場合、内部状態は結合されません（つまり、クエリは可能な限り効率的に動作しません）。この場合、[quantilesExactExclusive](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantilesexactexclusive) 関数を使用してください。

**構文**

``` sql
quantileExactExclusive(level)(expr)
```

**引数**

- `expr` — 数値型の[データタイプ](../../../sql-reference/data-types/index.md#data_types)、[Date](../../../sql-reference/data-types/date.md)、または[DateTime](../../../sql-reference/data-types/datetime.md)に結果するカラム値に対する式。

**パラメーター**

- `level` — 分位数のレベル。オプション。(0, 1) — 範囲は含まれません。デフォルト値: 0.5。`level=0.5` の場合、関数は[中央値](https://en.wikipedia.org/wiki/Median)を計算します。[Float](../../../sql-reference/data-types/float.md)。

**返される値**

- 指定されたレベルの分位数。

タイプ:

- 数値データ型入力の場合は [Float64](../../../sql-reference/data-types/float.md)。
- 入力値が `Date` 型の場合は [Date](../../../sql-reference/data-types/date.md)。
- 入力値が `DateTime` 型の場合は [DateTime](../../../sql-reference/data-types/datetime.md)。

**例**

クエリ:

``` sql
CREATE TABLE num AS numbers(1000);

SELECT quantileExactExclusive(0.6)(x) FROM (SELECT number AS x FROM num);
```

結果:

``` text
┌─quantileExactExclusive(0.6)(x)─┐
│                          599.6 │
└────────────────────────────────┘
```

## quantileExactInclusive

数値データ列の[分位数](https://en.wikipedia.org/wiki/Quantile)を正確に計算します。

正確な値を取得するために、渡された全ての値は配列に結合され、部分的にソートされます。そのため、関数は `O(n)` のメモリを消費し、ここで `n` は渡された値の数です。ただし、少数の値の場合、この関数は非常に効果的です。

この関数は、Excel の [PERCENTILE.INC](https://support.microsoft.com/en-us/office/percentile-inc-function-680f9539-45eb-410b-9a5e-c1355e5fe2ed) 関数と同等であり、（[タイプ R7](https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample)）です。

異なるレベルの `quantileExactInclusive` 関数をクエリで使用する場合、内部状態は結合されません（つまり、クエリは可能な限り効率的に動作しません）。この場合、[quantilesExactInclusive](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantilesexactinclusive) 関数を使用してください。

**構文**

``` sql
quantileExactInclusive(level)(expr)
```

**引数**

- `expr` — 数値型の[データタイプ](../../../sql-reference/data-types/index.md#data_types)、[Date](../../../sql-reference/data-types/date.md)、または[DateTime](../../../sql-reference/data-types/datetime.md)に結果するカラム値に対する式。

**パラメーター**

- `level` — 分位数のレベル。オプション。[0, 1] — 範囲は含まれます。デフォルト値: 0.5。`level=0.5` の場合、関数は[中央値](https://en.wikipedia.org/wiki/Median)を計算します。[Float](../../../sql-reference/data-types/float.md)。

**返される値**

- 指定されたレベルの分位数。

タイプ:

- 数値データ型入力の場合は [Float64](../../../sql-reference/data-types/float.md)。
- 入力値が `Date` 型の場合は [Date](../../../sql-reference/data-types/date.md)。
- 入力値が `DateTime` 型の場合は [DateTime](../../../sql-reference/data-types/datetime.md)。

**例**

クエリ:

``` sql
CREATE TABLE num AS numbers(1000);

SELECT quantileExactInclusive(0.6)(x) FROM (SELECT number AS x FROM num);
```

結果:

``` text
┌─quantileExactInclusive(0.6)(x)─┐
│                          599.4 │
└────────────────────────────────┘
```

**関連項目**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
