---
slug: /en/sql-reference/functions/time-series-functions
sidebar_position: 172
sidebar_label: Time Series
---

# Time Series Functions

Below functions are used for time series analysis.

## seriesOutliersTukey

Detects a possible anomaly in series using [Tukey Fences](https://en.wikipedia.org/wiki/Outlier#Tukey%27s_fences).

**Syntax**

``` sql
seriesOutliersTukey(series);
```

**Arguments**

- `series` - An array of numeric values

**Returned value**

- Returns an array of the same length where each value represents a modified Z-score of possible anomaly of corresponding element in the series.
- A value greater than 3 or lesser than -3 indicates a possible anomaly.

Type: [Array](../../sql-reference/data-types/array.md).

**Examples**

Query:

``` sql
seriesOutliersTukey([-3,2.4,15,3.9,5,6,4.5,5.2,3,4,5,16,7,5,5,4]) AS print_0;
```

Result:

``` text
┌───────────print_0──────────────────────────────────────────────────────────────────┐
│[-2.7121212121212137,0,4.196969696969699,0,0,0,0,0,0,0,0,4.803030303030305,0,0,0,0] │
└────────────────────────────────────────────────────────────────────────────────────┘
```

Query:

``` sql
seriesOutliersTukey(arrayMap(x -> sin(x / 10), range(30))) AS print_0;
```

Result:

``` text
┌───────────print_0────────────────────────────────────────────┐
│[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0] │
└──────────────────────────────────────────────────────────────┘
```

## seriesPeriodDetectFFT

Finds the period of the given time series data using FFT
FFT - [Fast Fourier transform](https://en.wikipedia.org/wiki/Fast_Fourier_transform)

**Syntax**

``` sql
seriesPeriodDetectFFT(series);
```

**Arguments**

- `series` - An array of numeric values

**Returned value**

- A real value equal to the period of time series
- Returns NAN when number of data points are less than four.

Type: [Float64](../../sql-reference/data-types/float.md).

**Examples**

Query:

``` sql
SELECT seriesPeriodDetectFFT([1, 4, 6, 1, 4, 6, 1, 4, 6, 1, 4, 6, 1, 4, 6, 1, 4, 6, 1, 4, 6]) AS print_0;
```

Result:

``` text
┌───────────print_0──────┐
│                      3 │
└────────────────────────┘
```

``` sql
SELECT seriesPeriodDetectFFT(arrayMap(x -> abs((x % 6) - 3), range(1000))) AS print_0;
```

Result:

``` text
┌─print_0─┐
│       6 │
└─────────┘
```

## seriesDecomposeSTL

Decomposes a time series using STL [(Seasonal-Trend Decomposition Procedure Based on Loess)](https://www.wessa.net/download/stl.pdf) into a season, a trend and a residual component. 

**Syntax**

``` sql
seriesDecomposeSTL(series, period);
```

**Arguments**

- `series` - An array of numeric values
- `period` - A positive integer

The number of data points in `series` should be at least twice the value of `period`.

**Returned value**

- An array of four arrays where the first array include seasonal components, the second array - trend,
the third array - residue component, and the fourth array - baseline(seasonal + trend) component.

Type: [Array](../../sql-reference/data-types/array.md).

**Examples**

Query:

``` sql
SELECT seriesDecomposeSTL([10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34], 3) AS print_0;
```

Result:

``` text
┌───────────print_0──────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [[
        -13.529999, -3.1799996, 16.71,      -13.53,     -3.1799996, 16.71,      -13.53,     -3.1799996,
        16.71,      -13.530001, -3.18,      16.710001,  -13.530001, -3.1800003, 16.710001,  -13.530001,
        -3.1800003, 16.710001,  -13.530001, -3.1799994, 16.71,      -13.529999, -3.1799994, 16.709997
    ],
    [
        23.63,     23.63,     23.630003, 23.630001, 23.630001, 23.630001, 23.630001, 23.630001,
        23.630001, 23.630001, 23.630001, 23.63,     23.630001, 23.630001, 23.63,     23.630001,
        23.630001, 23.63,     23.630001, 23.630001, 23.630001, 23.630001, 23.630001, 23.630003
    ],
    [
        0, 0.0000019073486, -0.0000019073486, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -0.0000019073486, 0,
        0
    ],
    [
        10.1, 20.449999, 40.340004, 10.100001, 20.45, 40.34, 10.100001, 20.45, 40.34, 10.1, 20.45, 40.34,
        10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.100002, 20.45, 40.34
    ]]                                                                                                                   │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```
