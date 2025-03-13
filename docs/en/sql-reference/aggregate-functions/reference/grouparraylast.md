---
slug: /en/sql-reference/aggregate-functions/reference/grouparraylast
sidebar_position: 142
---

# groupArrayLast

Syntax: `groupArrayLast(max_size)(x)`

Creates an array of last argument values.
For example, `groupArrayLast(1)(x)` is equivalent to `[anyLast (x)]`.

In some cases, you can still rely on the order of execution. This applies to cases when `SELECT` comes from a subquery that uses `ORDER BY` if the subquery result is small enough.

**Example**

Query:

```sql
select groupArrayLast(2)(number+1) numbers from numbers(10)
```

Result:

```text
┌─numbers─┐
│ [9,10]  │
└─────────┘
```

In compare to `groupArray`:

```sql
select groupArray(2)(number+1) numbers from numbers(10)
```

```text
┌─numbers─┐
│ [1,2]   │
└─────────┘
```
