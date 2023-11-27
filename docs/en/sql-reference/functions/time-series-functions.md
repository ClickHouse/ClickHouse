---
slug: /en/sql-reference/functions/time-series-functions
sidebar_position: 172
sidebar_label: Time Series
---

# Time Series Functions

Below functions are used for time series analysis.

## seriesPeriodDetectFFT

Finds the period of the given time series data using FFT
Detect Period in time series data using FFT.
FFT - Fast Fourier transform (https://en.wikipedia.org/wiki/Fast_Fourier_transform)

**Syntax**

``` sql
seriesPeriodDetectFFT(series);
```

**Arguments**

- `series` - An array of numeric values

**Returned value**

- A real value equal to the period of time series

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
