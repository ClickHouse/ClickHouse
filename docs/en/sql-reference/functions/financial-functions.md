---
description: 'Documentation for Financial Functions'
sidebar_label: 'Financial'
slug: /sql-reference/functions/financial-functions
title: 'Financial Functions'
---

# Financial functions

## financialInternalRateOfReturn {#financialInternalRateOfReturn}

Calculates the Internal Rate of Return (IRR) for a series of cash flows occurring at regular intervals. IRR is the discount rate at which the Net Present Value (NPV) equals zero.

IRR attempts to solve the following equation:

$$
\sum_{i=0}^n \frac{cashflow_i}{(1 + irr)^i} = 0
$$

**Syntax**

```sql
financialInternalRateOfReturn(cashflows[, guess])
```

**Arguments**

- `cashflows` — Array of cash flows. Each value represents a payment (negative value) or income (positive value). Type: Array of numeric values (Int8, Int16, Int32, Int64, Float32, Float64).
- `guess` — Optional initial guess (constant value) for the internal rate of return. Default: 0.1. Type: Float32|Float64.

**Returned value**

- Returns the internal rate of return as a Float64 value.
- Returns NaN if:
  - The calculation cannot converge
  - Input array is empty or has only one element
  - All cash flows are zero
  - Other calculation errors occur

**Examples**

Basic usage:
```sql
SELECT financialInternalRateOfReturn([-100000, 25000, 25000, 25000, 25000, 25000]);
```
```text
┌─financialInt_00, 25000])─┐
│      0.07930826116052862 │
└──────────────────────────┘
```

With initial guess:
```sql
SELECT financialInternalRateOfReturn([-100, 60, 60], 0.2);
```
```text
┌─financialInt_, 60], 0.2)─┐
│      0.13066238629180732 │
└──────────────────────────┘
```

**Notes**

- The function uses Newton-Raphson and TOMS748 methods for finding the root.
- At least one cash flow must be negative and one must be positive for a meaningful IRR calculation.


## financialInternalRateOfReturnExtended {#financialInternalRateOfReturnExtended}

Calculates the Extended Internal Rate of Return (XIRR) for a series of cash flows occurring at irregular intervals. XIRR is the discount rate at which the net present value (NPV) of all cash flows equals zero.

XIRR attempts to solve the following equation (example for `ACT_365F`):

$$
\sum_{i=0}^n \frac{cashflow_i}{(1 + rate)^{(date_i - date_0)/365}} = 0
$$

**Syntax**

```sql
financialInternalRateOfReturnExtended(cashflows, dates[, guess[, daycount]])
```

**Arguments**

- `cashflows` — Array of cash flows. Each value represents a payment (negative value) or income (positive value). Type: Array of numeric values (Int8, Int16, Int32, Int64, Float32, Float64).
- `dates` — Array of dates corresponding to each cash flow. Must be sorted in ascending order with unique values. Type: Array of Date or Date32.
- `guess` — Optional initial guess (constant value) for the internal rate of return. Default: 0.1. Type: Float32|Float64.
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
SELECT financialInternalRateOfReturnExtended(
    [-10000, 5750, 4250, 3250],
    [toDate('2020-01-01'), toDate('2020-03-01'), toDate('2020-10-30'), toDate('2021-02-15')]
);
```
```text
┌─financialInt_1-02-15')])─┐
│       0.6342972615260243 │
└──────────────────────────┘
```

Using different day count convention:
```sql
SELECT round(
    financialInternalRateOfReturnExtended([100000, -110000],
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

## financialNetPresentValueExtended {#financialNetPresentValueExtended}

Calculates the Extended Net Present Value (XNPV) for a series of cash flows occurring at irregular intervals. XNPV considers the specific timing of each cash flow when calculating present value.

XNPV equation for `ACT_365F`:

$$
XNPV=\sum_{i=1}^n \frac{cashflow_i}{(1 + rate)^{(date_i - date_0)/365}}
$$

**Syntax**

```sql
financialNetPresentValueExtended(rate, cashflows, dates[, daycount])
```

**Arguments**

- `rate` — The discount rate to apply. Type: Float64.
- `cashflows` — Array of cash flows. Each value represents a payment (negative value) or income (positive value). Must contain at least one positive and one negative value. Type: Array of numeric values (Int8, Int16, Int32, Int64, Float32, Float64).
- `dates` — Array of dates corresponding to each cash flow. Must have the same size as cashflows array. Type: Array of Date or Date32.
- `daycount` — Optional day count convention. Supported values:
  - 'ACT_365F' (default) — Actual/365 Fixed
  - 'ACT_365_25' — Actual/365.25

**Returned value**

- Returns the net present value as a Float64 value.

**Examples**

Basic usage:
```sql
SELECT financialNetPresentValueExtended(0.1, [-10_000., 5750., 4250., 3250.], 
    [toDate('2020-01-01'), toDate('2020-03-01'), toDate('2020-10-30'), toDate('2021-02-15')]);
```
```text
┌─financialNet_1-02-15')])─┐
│        2506.579458169746 │
└──────────────────────────┘
```

Using different day count convention:
```sql
SELECT financialNetPresentValueExtended(0.1, [-10_000., 5750., 4250., 3250.], 
    [toDate('2020-01-01'), toDate('2020-03-01'), toDate('2020-10-30'), toDate('2021-02-15')], 
    'ACT_365_25');
```
```text
┌─financialNet_CT_365_25')─┐
│        2507.067268742502 │
└──────────────────────────┘
```

## financialNetPresentValue {#financialNetPresentValue}

Calculates the Net Present Value (NPV) of a series of cash flows assuming equal time intervals between each cash flow.

Default variant:

$$
\sum_{i=0}^{N-1} \frac{values_i}{(1 + rate)^i}
$$

Variant with `start_from_zero` set to `False` (Excel style):

$$
\sum_{i=1}^{N} \frac{values_i}{(1 + rate)^i}
$$

**Syntax**

```sql
financialNetPresentValue(rate, cashflows[, start_from_zero])
```

**Arguments**

- `rate` — The discount rate to apply. Type: Float64.
- `cashflows` — Array of cash flows. Each value represents a payment (negative value) or income (positive value). Type: Array of numeric values (Int8, Int16, Int32, Int64, Float32, Float64).
- `start_from_zero` — Optional boolean parameter indicating whether to start the NPV calculation from period 0 (true) or period 1 (false). Default: true.

**Returned value**

- Returns the net present value as a Float64 value.

**Examples**

Basic usage:
```sql
SELECT financialNetPresentValue(0.08, [-40_000., 5_000., 8_000., 12_000., 30_000.]);
```
```text
┌─financialNet_., 30000.])─┐
│       3065.2226681795255 │
└──────────────────────────┘
```

With start_from_zero = false (Excel compatibility mode):
```sql
SELECT financialNetPresentValue(0.08, [-40_000., 5_000., 8_000., 12_000., 30_000.], False);
```
```text
┌─financialNet_30000.], 0)─┐
│       2838.1691372032656 │
└──────────────────────────┘
```

**Notes**

- When `start_from_zero = true`, the first cash flow is discounted by `(1 + rate)^0`, which equals 1
- When `start_from_zero = false`, the first cash flow is discounted by `(1 + rate)^1`, matching Excel's NPV function behavior
