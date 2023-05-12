---
sidebar_position: 106
---

# argMax

Calculates the `arg` value for a maximum `val` value. If there are several different values of `arg` for maximum values of `val`, returns the first of these values encountered.

**Syntax**

``` sql
argMax(arg, val)
```

**Arguments**

-   `arg` — Argument.
-   `val` — Value.

**Returned value**

-   `arg` value that corresponds to maximum `val` value.

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
SELECT argMax(user, salary) FROM salary;
```

Result:

``` text
┌─argMax(user, salary)─┐
│ director             │
└──────────────────────┘
```
