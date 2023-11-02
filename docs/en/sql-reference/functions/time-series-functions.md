---
slug: /en/sql-reference/functions/time-series-functions
sidebar_position: 140
sidebar_label: Time Series
---

# Time Series Functions

Below functions are used for time series anlaysis.

## seriesPeriodDetect

Finds the period of the given time series data

**Syntax**

``` sql
seriesPeriodDetect(series);
```

**Arguments**

- `series` - An array of numeric values

**Returned value**

- A real value equal to the period of time series

Type: [Float64](../../sql-reference/data-types/float.md).

**Examples**

Query:

``` sql
SELECT seriesPeriodDetect([1,4,6,1,4,6,1,4,6,1,4,6]) AS print_0;
```

Result:

``` text
┌───────────print_0──────┐
│                      3 │
└────────────────────────┘
```