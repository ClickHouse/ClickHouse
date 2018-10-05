# Conditional functions

## if(cond, then, else), cond ? operator then : else

Returns `then` if `cond != 0`, or `else` if `cond = 0`.
`cond` must be of type `UInt8`, and `then` and `else` must have the lowest common type.

`then` and `else` can be `NULL`

## multiIf

Allows you to write the [CASE](../operators.md#operator_case) operator more compactly in the query.

```
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

```
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Run the query `SELECT multiIf(isNull(y) x, y < 3, y, NULL) FROM t_null`. Result:

```
┌─multiIf(isNull(y), x, less(y, 3), y, NULL)─┐
│                                          1 │
│                                       ᴺᵁᴸᴸ │
└────────────────────────────────────────────┘
```
