---
description: 'Returns the sum of exponentially smoothed moving average values of a
  time series at the index `t` in time.'
sidebar_position: 136
slug: /sql-reference/aggregate-functions/reference/exponentialTimeDecayedSum
title: 'exponentialTimeDecayedSum'
---

## exponentialTimeDecayedSum {#exponentialtimedecayedsum}

Returns the sum of exponentially smoothed moving average values of a time series at the index `t` in time.

**Syntax**

```sql
exponentialTimeDecayedSum(x)(v, t)
```

**Arguments**

- `v` — Value. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) or [Decimal](../../../sql-reference/data-types/decimal.md).
- `t` — Time. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) or [Decimal](../../../sql-reference/data-types/decimal.md), [DateTime](../../data-types/datetime.md), [DateTime64](../../data-types/datetime64.md).

**Parameters**

- `x` — Time difference required for a value's weight to decay to 1/e. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) or [Decimal](../../../sql-reference/data-types/decimal.md).

**Returned values**

- Returns the sum of exponentially smoothed moving average values at the given point in time. [Float64](../../data-types/float.md).

**Example**

Query:

```sql
SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 10, 50) AS bar
FROM
    (
    SELECT
    (number = 0) OR (number >= 25) AS value,
    number AS time,
    exponentialTimeDecayedSum(10)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
    );
```

Result:

```response
    ┌─value─┬─time─┬─round(exp_smooth, 3)─┬─bar───────────────────────────────────────────────┐
 1. │     1 │    0 │                    1 │ █████                                             │
 2. │     0 │    1 │                0.905 │ ████▌                                             │
 3. │     0 │    2 │                0.819 │ ████                                              │
 4. │     0 │    3 │                0.741 │ ███▋                                              │
 5. │     0 │    4 │                 0.67 │ ███▎                                              │
 6. │     0 │    5 │                0.607 │ ███                                               │
 7. │     0 │    6 │                0.549 │ ██▋                                               │
 8. │     0 │    7 │                0.497 │ ██▍                                               │
 9. │     0 │    8 │                0.449 │ ██▏                                               │
10. │     0 │    9 │                0.407 │ ██                                                │
11. │     0 │   10 │                0.368 │ █▊                                                │
12. │     0 │   11 │                0.333 │ █▋                                                │
13. │     0 │   12 │                0.301 │ █▌                                                │
14. │     0 │   13 │                0.273 │ █▎                                                │
15. │     0 │   14 │                0.247 │ █▏                                                │
16. │     0 │   15 │                0.223 │ █                                                 │
17. │     0 │   16 │                0.202 │ █                                                 │
18. │     0 │   17 │                0.183 │ ▉                                                 │
19. │     0 │   18 │                0.165 │ ▊                                                 │
20. │     0 │   19 │                 0.15 │ ▋                                                 │
21. │     0 │   20 │                0.135 │ ▋                                                 │
22. │     0 │   21 │                0.122 │ ▌                                                 │
23. │     0 │   22 │                0.111 │ ▌                                                 │
24. │     0 │   23 │                  0.1 │ ▌                                                 │
25. │     0 │   24 │                0.091 │ ▍                                                 │
26. │     1 │   25 │                1.082 │ █████▍                                            │
27. │     1 │   26 │                1.979 │ █████████▉                                        │
28. │     1 │   27 │                2.791 │ █████████████▉                                    │
29. │     1 │   28 │                3.525 │ █████████████████▋                                │
30. │     1 │   29 │                 4.19 │ ████████████████████▉                             │
31. │     1 │   30 │                4.791 │ ███████████████████████▉                          │
32. │     1 │   31 │                5.335 │ ██████████████████████████▋                       │
33. │     1 │   32 │                5.827 │ █████████████████████████████▏                    │
34. │     1 │   33 │                6.273 │ ███████████████████████████████▎                  │
35. │     1 │   34 │                6.676 │ █████████████████████████████████▍                │
36. │     1 │   35 │                7.041 │ ███████████████████████████████████▏              │
37. │     1 │   36 │                7.371 │ ████████████████████████████████████▊             │
38. │     1 │   37 │                7.669 │ ██████████████████████████████████████▎           │
39. │     1 │   38 │                7.939 │ ███████████████████████████████████████▋          │
40. │     1 │   39 │                8.184 │ ████████████████████████████████████████▉         │
41. │     1 │   40 │                8.405 │ ██████████████████████████████████████████        │
42. │     1 │   41 │                8.605 │ ███████████████████████████████████████████       │
43. │     1 │   42 │                8.786 │ ███████████████████████████████████████████▉      │
44. │     1 │   43 │                 8.95 │ ████████████████████████████████████████████▊     │
45. │     1 │   44 │                9.098 │ █████████████████████████████████████████████▍    │
46. │     1 │   45 │                9.233 │ ██████████████████████████████████████████████▏   │
47. │     1 │   46 │                9.354 │ ██████████████████████████████████████████████▊   │
48. │     1 │   47 │                9.464 │ ███████████████████████████████████████████████▎  │
49. │     1 │   48 │                9.563 │ ███████████████████████████████████████████████▊  │
50. │     1 │   49 │                9.653 │ ████████████████████████████████████████████████▎ │
    └───────┴──────┴──────────────────────┴───────────────────────────────────────────────────┘
```
