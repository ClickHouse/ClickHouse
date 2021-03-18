---
toc_priority: 105
---

# argMin {#agg-function-argmin}

Syntax: `argMin(arg, val)` or `argMin(tuple(arg, val))`

Calculates the `arg` value for a minimal `val` value. If there are several different values of `arg` for minimal values of `val`, the first of these values encountered is output.

Tuple version of this function will return the tuple with the minimal `val` value. It is convinient for use with `SimpleAggregateFunction`.

**Example:**

``` text
┌─user─────┬─salary─┐
│ director │   5000 │
│ manager  │   3000 │
│ worker   │   1000 │
└──────────┴────────┘
```

``` sql
SELECT argMin(user, salary), argMin(tuple(user, salary)) FROM salary
```

``` text
┌─argMin(user, salary)─┬─argMin(tuple(user, salary))─┐
│ worker               │ ('worker',1000)             │
└──────────────────────┴─────────────────────────────┘
```
