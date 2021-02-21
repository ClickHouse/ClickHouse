---
toc_priority: 105
---

# argMin {#agg-function-argmin}

Syntax: `argMin(arg, val)`

Calculates the `arg` value for a minimal `val` value. If there are several different values of `arg` for minimal values of `val`, the first of these values encountered is output.

**Example:**

``` text
┌─user─────┬─salary─┐
│ director │   5000 │
│ manager  │   3000 │
│ worker   │   1000 │
└──────────┴────────┘
```

``` sql
SELECT argMin(user, salary) FROM salary
```

``` text
┌─argMin(user, salary)─┐
│ worker               │
└──────────────────────┘
```
