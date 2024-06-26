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
- `min_percentile` - The minimum percentile to be used to calculate inter-quantile range [(IQR)](https://en.wikipedia.org/wiki/Interquartile_range). The value must be in range [0.02,0.98]. The default is 0.25.
- `max_percentile` - The maximum percentile to be used to calculate inter-quantile range (IQR). The value must be in range [0.02,0.98]. The default is 0.75.
- `K` - Non-negative constant value to detect mild or stronger outliers. The default value is 1.5.

At least four data points are required in `series` to detect outliers.

**Returned value**

- Returns an array of the same length as the input array where each value represents score of possible anomaly of corresponding element in the series. A non-zero score indicates a possible anomaly. [Array](../data-types/array.md).

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
SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4.50, 5, 12, 45, 12, 3.40, 3, 4, 5, 6], 0.2, 0.8, 1.5) AS print_0;
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

- A real value equal to the period of series data. NaN when number of data points are less than four. [Float64](../data-types/float.md).

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
the third array - residue component, and the fourth array - baseline(seasonal + trend) component. [Array](../data-types/array.md).

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

## autoregress

An autoregressive function is a fundamental concept in time series analysis and modeling, representing a statistical technique used to predict future values based on past observations(prior samples). This function encapsulates the idea that the present value of a time series is a function of its past values. The general framework of an autoregressive (AR) function can accommodate both linear and nonlinear relationships, each serving distinct purposes.

**Linear Autoregressive Functions**

This model assumes that the current value of a time series can be expressed as a linear combination of its previous "p" values plus a stochastic error term:

$$
y_t = c + \phi_1y_{t-1} + \phi_2y_{t-2} + \ldots + \phi_py_{t-p} + \epsilon_t
$$

Where:
- $y_t$ is the value at time $t$.
-  $c$ is a constant.
- $\phi_i$ are parameters for each lag.
- $\epsilon_t$ is the error term, assumed to be white noise.

**Nonlinear Autoregressive Functions**

While linear autoregressive models are powerful tools, they may not fully capture the complexities inherent in many real-world time series. Nonlinear autoregressive functions extend the capabilities by allowing for more sophisticated dynamics between past and present values. 

For instance, a nonlinear autoregressive function might be represented as:

$$
y_t = f(y_{t-1}, y_{t-2}, ..., y_{t-p}) + \epsilon_t 
$$

where $f$ is a nonlinear function. 

**Syntax**

Syntax 1 - single prior sample

``` sql
autoregress(x->{expression}, backward_offset, initial_value)
```

or

Syntax 2 - multiple prior samples

``` sql
autoregress( x1,x2,...,x_n-> expr , backward_offsets, inital_values )
```

**Arguments**

For syntax 1:
- `x->{expression}` - A function that takes a single prior sample variable as input and returns a value.
- `backward_offset` - A backward offset at which to retrieve the prior sample, i.e., $f(t - backward\_offset )$.
- `initial_value` - An initial value for the autoregressive function.
  
For syntax 2:
- `x1,x2,...,x_n->{expression}` - A function that takes several prior samples as input and returns a value.
- `backward_offsets` - A tuple of backward offsets at which to retrieve the prior samples, i.e., $f(t - backward\_offset\_1 )$, $f(t - backward\_offset\_2 )$, ..., $f(t - backward\_offset\_n )$.
- `inital_values` - A tuple of initial values for 

**Returned value**

- A Float64 result

**Example**

Query:

Generate Fibonacci sequence.

``` sql
select autoregress(x1,x2->toFloat64(x1+x2), (1,2), (toFloat64(1), toFloat64(0))) as fibo from numbers(20);
```

Result:

```
┌─fibo─┐
│    1 │
│    1 │
│    2 │
│    3 │
│    5 │
│    8 │
│   13 │
│   21 │
│   34 │
│   55 │
│   89 │
│  144 │
│  233 │
│  377 │
│  610 │
│  987 │
│ 1597 │
│ 2584 │
│ 4181 │
│ 6765 │
└──────┘
```
