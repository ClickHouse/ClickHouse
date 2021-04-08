---
toc_priority: 106
---

# argMax {#agg-function-argmax}

Syntax: `argMax(arg, val)` or `argMax(tuple(arg, val))`

Calculates the `arg` value for a maximum `val` value. If there are several different values of `arg` for maximum values of `val`, the first of these values encountered is output.

Tuple version of this function will return the tuple with the maximum `val` value. It is convinient for use with `SimpleAggregateFunction`.

**Example:**

``` text
┌─user─────┬─salary─┐
│ director │   5000 │
│ manager  │   3000 │
│ worker   │   1000 │
└──────────┴────────┘
```

``` sql
SELECT argMax(user, salary), argMax(tuple(user, salary)) FROM salary
```

``` text
┌─argMax(user, salary)─┬─argMax(tuple(user, salary))─┐
│ director             │ ('director',5000)           │
└──────────────────────┴─────────────────────────────┘
```
