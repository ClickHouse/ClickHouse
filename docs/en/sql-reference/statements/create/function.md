---
description: 'Documentation for Function'
sidebar_label: 'FUNCTION'
sidebar_position: 38
slug: /sql-reference/statements/create/function
title: 'CREATE FUNCTION -user defined function (UDF)'
doc_type: 'reference'
---

Creates a user defined function (UDF) from a lambda expression. The expression must consist of function parameters, constants, operators, or other function calls.

**Syntax**

```sql
CREATE [OR REPLACE] FUNCTION name [ON CLUSTER cluster] AS (parameter0, ...) -> expression
```
A function can have an arbitrary number of parameters.

There are a few restrictions:

- The name of a function must be unique among user defined and system functions.
- Recursive functions are not allowed.
- All variables used by a function must be specified in its parameter list.

If any restriction is violated then an exception is raised.

**Example**

```sql title="Query"
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
SELECT number, linear_equation(number, 2, 1) FROM numbers(3);
```

```text title="Response"
┌─number─┬─plus(multiply(2, number), 1)─┐
│      0 │                            1 │
│      1 │                            3 │
│      2 │                            5 │
└────────┴──────────────────────────────┘
```

A [conditional function](../../../sql-reference/functions/conditional-functions.md) is called in a user defined function in the following query:

```sql title="Query"
CREATE FUNCTION parity_str AS (n) -> if(n % 2, 'odd', 'even');
SELECT number, parity_str(number) FROM numbers(3);
```

```text title="Response"
┌─number─┬─if(modulo(number, 2), 'odd', 'even')─┐
│      0 │ even                                 │
│      1 │ odd                                  │
│      2 │ even                                 │
└────────┴──────────────────────────────────────┘
```

Replace an existing UDF:

```sql title="Query"
CREATE FUNCTION exampleReplaceFunction AS frame -> frame;
SELECT create_query FROM system.functions WHERE name = 'exampleReplaceFunction';
CREATE OR REPLACE FUNCTION exampleReplaceFunction AS frame -> frame + 1;
SELECT create_query FROM system.functions WHERE name = 'exampleReplaceFunction';
```

```text title="Response"
┌─create_query─────────────────────────────────────────────┐
│ CREATE FUNCTION exampleReplaceFunction AS frame -> frame │
└──────────────────────────────────────────────────────────┘

┌─create_query───────────────────────────────────────────────────┐
│ CREATE FUNCTION exampleReplaceFunction AS frame -> (frame + 1) │
└────────────────────────────────────────────────────────────────┘
```

## Related Content {#related-content}

### [Executable UDFs](/sql-reference/functions/udf.md). {#executable-udfs}

### [User-defined functions in ClickHouse Cloud](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs) {#user-defined-functions-in-clickhouse-cloud}
