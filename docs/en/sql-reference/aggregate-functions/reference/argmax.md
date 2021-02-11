---
toc_priority: 106
---

# argMax {#agg-function-argmax}

Calculates the `arg` value for a maximum `val` value. If there are several different values of `arg` for maximum values of `val`, returns the first of these values encountered.

Tuple version of this function will return the tuple with the maximum `val` value. It is convenient for use with [SimpleAggregateFunction](../../../sql-reference/data-types/simpleaggregatefunction.md).

**Syntax**

``` sql
argMax(arg, val)
```

or

``` sql
argMax(tuple(arg, val))
```

**Parameters**

-   `arg` — Argument.
-   `val` — Value.

**Returned value**

-   `arg` value that corresponds to maximum `val` value.

Type: matches `arg` type. 

For tuple in the input:

-   Tuple `(arg, val)`, where `val` is the maximum value and `arg` is a corresponding value.

Type: [Tuple](../../../sql-reference/data-types/tuple.md).

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
SELECT argMax(user, salary), argMax(tuple(user, salary)) FROM salary;
```

Result:

``` text
┌─argMax(user, salary)─┬─argMax(tuple(user, salary))─┐
│ director             │ ('director',5000)           │
└──────────────────────┴─────────────────────────────┘
```

[Original article](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/argmax/) <!--hide-->
