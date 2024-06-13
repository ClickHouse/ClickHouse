---
slug: /en/sql-reference/aggregate-functions/reference/exponentialtimedecayedavg
sidebar_position: 108
sidebar_title: exponentialTimeDecayedAvg
---

## exponentialTimeDecayedAvg

Calculates the exponential moving average of values for the determined time.

**Syntax**

```sql
exponentialTimeDecayedAvg(x)(value, timeunit)
```

**Examples**

Query:

```sql
SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 5, 50) AS bar
FROM
(
    SELECT
        (number = 0) OR (number >= 25) AS value,
        number AS time,
        exponentialTimeDecayedAvg(10)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
);
```

Response:

```sql
   ┌─value─┬─time─┬─round(exp_smooth, 3)─┬─bar────────┐
 1. │     1 │    0 │                    1 │ ██████████ │
 2. │     0 │    1 │                0.475 │ ████▊      │
 3. │     0 │    2 │                0.301 │ ███        │
 4. │     0 │    3 │                0.214 │ ██▏        │
 5. │     0 │    4 │                0.162 │ █▌         │
 6. │     0 │    5 │                0.128 │ █▎         │
 7. │     0 │    6 │                0.104 │ █          │
 8. │     0 │    7 │                0.086 │ ▊          │
 9. │     0 │    8 │                0.072 │ ▋          │
10. │     0 │    9 │                0.061 │ ▌          │
11. │     0 │   10 │                0.052 │ ▌          │
12. │     0 │   11 │                0.045 │ ▍          │
13. │     0 │   12 │                0.039 │ ▍          │
14. │     0 │   13 │                0.034 │ ▎          │
15. │     0 │   14 │                 0.03 │ ▎          │
16. │     0 │   15 │                0.027 │ ▎          │
17. │     0 │   16 │                0.024 │ ▏          │
18. │     0 │   17 │                0.021 │ ▏          │
19. │     0 │   18 │                0.018 │ ▏          │
20. │     0 │   19 │                0.016 │ ▏          │
21. │     0 │   20 │                0.015 │ ▏          │
22. │     0 │   21 │                0.013 │ ▏          │
23. │     0 │   22 │                0.012 │            │
24. │     0 │   23 │                 0.01 │            │
25. │     0 │   24 │                0.009 │            │
26. │     1 │   25 │                0.111 │ █          │
27. │     1 │   26 │                0.202 │ ██         │
28. │     1 │   27 │                0.283 │ ██▊        │
29. │     1 │   28 │                0.355 │ ███▌       │
30. │     1 │   29 │                 0.42 │ ████▏      │
31. │     1 │   30 │                0.477 │ ████▊      │
32. │     1 │   31 │                0.529 │ █████▎     │
33. │     1 │   32 │                0.576 │ █████▊     │
34. │     1 │   33 │                0.618 │ ██████▏    │
35. │     1 │   34 │                0.655 │ ██████▌    │
36. │     1 │   35 │                0.689 │ ██████▉    │
37. │     1 │   36 │                0.719 │ ███████▏   │
38. │     1 │   37 │                0.747 │ ███████▍   │
39. │     1 │   38 │                0.771 │ ███████▋   │
40. │     1 │   39 │                0.793 │ ███████▉   │
41. │     1 │   40 │                0.813 │ ████████▏  │
42. │     1 │   41 │                0.831 │ ████████▎  │
43. │     1 │   42 │                0.848 │ ████████▍  │
44. │     1 │   43 │                0.862 │ ████████▌  │
45. │     1 │   44 │                0.876 │ ████████▊  │
46. │     1 │   45 │                0.888 │ ████████▉  │
47. │     1 │   46 │                0.898 │ ████████▉  │
48. │     1 │   47 │                0.908 │ █████████  │
49. │     1 │   48 │                0.917 │ █████████▏ │
50. │     1 │   49 │                0.925 │ █████████▏ │
    └───────┴──────┴──────────────────────┴────────────┘
```