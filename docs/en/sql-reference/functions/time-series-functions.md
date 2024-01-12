---
slug: /en/sql-reference/functions/time-series-functions
sidebar_position: 172
sidebar_label: Time Series
---

# Time Series Functions

Below functions are used for series data analysis.

## seriesOutliersDetectTukey

Detects outliers in series data using [Tukey Fences](https://en.wikipedia.org/wiki/Outlier#Tukey%27s_fences).

**Syntax**

``` sql
seriesOutliersDetectTukey(series);
seriesOutliersDetectTukey(series, min_percentile, max_percentile, K);
```

**Arguments**

- `series` - An array of numeric values.
- `min_percentile` - The minimum percentile to be used to calculate inter-quantile range [(IQR)](https://en.wikipedia.org/wiki/Interquartile_range). The value must be in range [2,98]. The default is 25.
- `max_percentile` - The maximum percentile to be used to calculate inter-quantile range (IQR). The value must be in range [2,98]. The default is 75.
- `K` - Non-negative constant value to detect mild or stronger outliers. The default value is 1.5.

At least four data points are required in `series` to detect outliers.

**Returned value**

- Returns an array of the same length as the input array where each value represents score of possible anomaly of corresponding element in the series. A non-zero score indicates a possible anomaly.

Type: [Array](../../sql-reference/data-types/array.md).

**Examples**

Query:

``` sql
SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4, 5, 12, 45, 12, 3, 3, 4, 5, 6]) AS print_0;
```

Result:

``` text
┌───────────print_0─────────────────┐
│[0,0,0,0,0,0,0,0,0,27,0,0,0,0,0,0] │
└───────────────────────────────────┘
```

Query:

``` sql
SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4.50, 5, 12, 45, 12, 3.40, 3, 4, 5, 6], 20, 80, 1.5) AS print_0;
```

Result:

``` text
┌─print_0──────────────────────────────┐
│ [0,0,0,0,0,0,0,0,0,19.5,0,0,0,0,0,0] │
└──────────────────────────────────────┘
```

## seriesPeriodDetectFFT

Finds the period of the given series data data using FFT
FFT - [Fast Fourier transform](https://en.wikipedia.org/wiki/Fast_Fourier_transform)

**Syntax**

``` sql
seriesPeriodDetectFFT(series);
```

**Arguments**

- `series` - An array of numeric values

**Returned value**

- A real value equal to the period of series data
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

Decomposes a series data using STL [(Seasonal-Trend Decomposition Procedure Based on Loess)](https://www.wessa.net/download/stl.pdf) into a season, a trend and a residual component. 

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

## seriesDecomposeAnomaliesDetection

Detect anomalies in series data through [series decomposition](#seriesDecomposeSTL).

**Syntax**

``` sql
seriesDecomposeAnomaliesDetection(series);
seriesDecomposeAnomaliesDetection(series, threshold, seasonality, AD_method);
```

**Arguments**

- `series` - An array of numeric values
- `threshold` - A positive number to detect mild or stronger anomalies. The default is 1.5, K value.
- `Seasonality` - Represents the period for the seasonal analysis of series data. Supported values are:
                - `-1`: Autodetect period using [seriesPeriodDetectFFT](#seriesPeriodDetectFFT). This is the default value.
                - `0`: Set period to 0 to skip extracting seasonal component.
                - A positive integer representing the period of series.
- `AD_method` - The method to use for anomaly detection on residual component. Supported values are:
                - `tukey` : [Tukey fence](#seriesoutliersdetecttukey) test with standard 25th-75th percentile range.
                - `ctukey`: Tukey fence test with custom 10th-90th percentile range. This is the default value.

The number of data points in `series` should be at least twice the value of `period`.

**Returned value**

- An array of three arrays where the first array includes a ternary series containing (+1,-1,0) marking up/down/no anomaly respectively, the second array - anomaly score,
the third array - baseline(seasonal + trend) component.

Type: [Array](../../sql-reference/data-types/array.md).

**Examples**

Query:

``` sql
SELECT seriesDecomposeAnomaliesDetection([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], 1.5, -1, 'tukey') AS print_0;
```

Result:

``` text
┌───────────print_0─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [[
        1, 0, 0, 0, 0, 0, 1, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, -1, 0
    ],
    [
        2.384185791015625e-7, 0, 0, 0, 0, 0, 2.384185791015625e-7, 2.384185791015625e-7, 0, 2.384185791015625e-7,
        0, 0, 0, 0, 2.384185791015625e-7, 0, 0, 0, 0, -2.384185791015625e-7, 0
    ],
    [
        3.999999761581421, 3, 2, 4, 3, 2, 4, 2.999999761581421, 1.9999998807907104, 4, 3, 2, 4,
        3,1.9999996423721313, 4, 3, 2, 4, 3.000000238418579, 2]
    ]                                                                                                                   │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```
