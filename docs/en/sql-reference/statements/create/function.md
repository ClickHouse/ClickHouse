---
sidebar_position: 38
sidebar_label: FUNCTION
---

# CREATE FUNCTION

Creates a user defined function from a lambda expression. The expression must consist of function parameters, constants, operators, or other function calls.

**Syntax**

```sql
CREATE FUNCTION name [ON CLUSTER cluster] AS (parameter0, ...) -> expression
```
A function can have an arbitrary number of parameters.

There are a few restrictions:

-   The name of a function must be unique among user defined and system functions.
-   Recursive functions are not allowed.
-   All variables used by a function must be specified in its parameter list.

If any restriction is violated then an exception is raised.

**Example**

Query:

```sql
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
SELECT number, linear_equation(number, 2, 1) FROM numbers(3);
```

Result:

``` text
┌─number─┬─plus(multiply(2, number), 1)─┐
│      0 │                            1 │
│      1 │                            3 │
│      2 │                            5 │
└────────┴──────────────────────────────┘
```

A [conditional function](../../../sql-reference/functions/conditional-functions.md) is called in a user defined function in the following query:

```sql
CREATE FUNCTION parity_str AS (n) -> if(n % 2, 'odd', 'even');
SELECT number, parity_str(number) FROM numbers(3);
```

Result:

``` text
┌─number─┬─if(modulo(number, 2), 'odd', 'even')─┐
│      0 │ even                                 │
│      1 │ odd                                  │
│      2 │ even                                 │
└────────┴──────────────────────────────────────┘
```
