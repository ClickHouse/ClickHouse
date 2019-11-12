# Conditional functions

## if {#if}

Controls conditional branching. Unlike most systems, ClickHouse considers the condition `cond` and expressions `then` and `else`. This is needed to consider answer quickly.

**Syntax** 

```sql
SELECT if(cond, then, else)
```

If the condition `cond` evaluates to a non-zero value, returns the result of the expression `then`, and the result of the expression `else`, if present, is skipped. If the `cond` is zero or `NULL`, then the result of the `then` expression is skipped and the result of the `else` expression, if present, is returned.

**Parameters**

- `cond` – The condition for evaluation that can be zero or not. Can be [UInt8](../../data_types/int_uint.md) or `NULL`.
- `then` - The expression to return if condition is met.
- `else` - The expression to return if condition is not met.

**Returned values**

The function executes `then` and `else` expressions and returns its result, depending on whether the condition `cond` ended up being zero or not.

**Example**

Query:

```sql
SELECT if(1, plus(2, 2), plus(2, 6))
```

Result:

```text
┌─plus(2, 2)─┐
│          4 │
└────────────┘
```

Query:

```sql
SELECT if(0, plus(2, 2), plus(2, 6))
```

Result:

```text
┌─plus(2, 6)─┐
│          8 │
└────────────┘
```

## multiIf

Allows you to write the [CASE](../operators.md#operator_case) operator more compactly in the query.

```sql
multiIf(cond_1, then_1, cond_2, then_2...else)
```

**Parameters:**

- `cond_N` — The condition for the function to return `then_N`.
- `then_N` — The result of the function when executed.
- `else` — The result of the function if none of the conditions is met.

The function accepts `2N+1` parameters.

**Returned values**

The function returns one of the values `then_N` or `else`, depending on the conditions `cond_N`.

**Example**

Take the table

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Run the query `SELECT multiIf(isNull(y) x, y < 3, y, NULL) FROM t_null`. Result:

```text
┌─multiIf(isNull(y), x, less(y, 3), y, NULL)─┐
│                                          1 │
│                                       ᴺᵁᴸᴸ │
└────────────────────────────────────────────┘
```

[Original article](https://clickhouse.yandex/docs/en/query_language/functions/conditional_functions/) <!--hide-->
