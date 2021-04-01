---
toc_priority: 105
---

# argMin {#agg-function-argmin}

Calculates the `arg` value for a minimum `val` value. If there are several different values of `arg` for minimum values of `val`, returns the first of these values encountered.

Tuple version of this function will return the tuple with the minimum `val` value. It is convenient for use with [SimpleAggregateFunction](../../../sql-reference/data-types/simpleaggregatefunction.md).

**Syntax**

``` sql
argMin(arg, val)
```

or

``` sql
argMin(tuple(arg, val))
```

**Arguments**

-   `arg` — Argument.
-   `val` — Value.

**Returned value**

-   `arg` value that corresponds to minimum `val` value.

Type: matches `arg` type. 

For tuple in the input:

-   Tuple `(arg, val)`, where `val` is the minimum value and `arg` is a corresponding value.

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
SELECT argMin(user, salary), argMin(tuple(user, salary)) FROM salary;
```

Result:

``` text
┌─argMin(user, salary)─┬─argMin(tuple(user, salary))─┐
│ worker               │ ('worker',1000)             │
└──────────────────────┴─────────────────────────────┘
```

[Original article](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/argmin/) <!--hide-->
