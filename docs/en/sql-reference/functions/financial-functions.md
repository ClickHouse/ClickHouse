---
description: 'Documentation for Financial Functions'
sidebar_label: 'Financial'
sidebar_position: 125
slug: /sql-reference/functions/financial-functions
title: 'Financial Functions'
---

# Financial Functions

## xirr {#xirr}

Calculates the Extended Internal Rate of Return (XIRR) for a series of cash flows occurring at irregular intervals. XIRR is the discount rate at which the net present value (NPV) of all cash flows equals zero.

**Syntax**

```sql
xirr(cashflows, dates[, guess[, daycount]])
```

**Arguments**

- `cashflows` — Array of cash flows. Each value represents a payment (negative value) or income (positive value). Type: Array of numeric values (Int8, Int16, Int32, Int64, Float32, Float64).
- `dates` — Array of dates corresponding to each cash flow. Must be sorted in ascending order with unique values. Type: Array of Date or Date32.
- `guess` — Optional initial guess for the internal rate of return. Default: 0.1. Type: Float64.
- `daycount` — Optional day count convention. Supported values:
  - 'ACT_365F' (default) — Actual/365 Fixed
  - 'ACT_365_25' — Actual/365.25

**Returned value**

- Returns the internal rate of return as a Float64 value.
- Returns NaN if:
  - The calculation cannot converge
  - Input arrays are empty or have only one element
  - All cash flows are zero
  - Dates are not sorted or not unique
  - Other calculation errors occur

**Examples**

Basic usage:
```sql
SELECT xirr(
    [-10000, 5750, 4250, 3250],
    [toDate('2020-01-01'), toDate('2020-03-01'), toDate('2020-10-30'), toDate('2021-02-15')]
);
```
```text
┌─xirr()─────────────┐
│ 0.6342972615260243 │
└────────────────────┘
```

Using different day count convention:
```sql
SELECT round(
    xirr([100000, -110000],
    [toDate('2020-01-01'), toDate('2021-01-01')],
    0.1,
    'ACT_365_25'
), 6) AS xirr_365_25;
```
```text
┌─xirr_365_25─┐
│    0.099785 │
└─────────────┘
```

**Notes**

- The function uses Newton-Raphson and TOMS748 methods for finding the root.
- The dates array must be sorted in ascending order with unique values.
