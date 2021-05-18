---
toc_priority: 105
---

# argMin {#agg-function-argmin}

Calculates the `arg` value for a minimum `val` value. If there are several different values of `arg` for minimum values of `val`, returns the first of these values encountered.

**Syntax**

``` sql
argMin(arg, val)
```

**Arguments**

-   `arg` — Argument.
-   `val` — Value.

**Returned value**

-   `arg` value that corresponds to minimum `val` value.

Type: matches `arg` type.

**Example**

Input table:

``` text
┌─user─────┬─salary─┐
│ director │   5000 │
│ manager  │   3000 │
│ worker   │   1000 │
└──────────┴────────┘
```

Query:

``` sql
SELECT argMin(user, salary) FROM salary
```

Result:

``` text
┌─argMin(user, salary)─┐
│ worker               │
└──────────────────────┘
```
