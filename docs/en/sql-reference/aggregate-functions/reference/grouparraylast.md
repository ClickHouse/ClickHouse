---
description: 'Creates an array of the last argument values.'
sidebar_position: 142
slug: /sql-reference/aggregate-functions/reference/grouparraylast
title: 'groupArrayLast'
---

# groupArrayLast

Syntax: `groupArrayLast(max_size)(x)`

Creates an array of the last argument values.
For example, `groupArrayLast(1)(x)` is equivalent to `[anyLast (x)]`.

In some cases, you can still rely on the order of execution. This applies to cases when `SELECT` comes from a subquery that uses `ORDER BY` if the subquery result is small enough.

**Example**

Query:

```sql
SELECT groupArrayLast(2)(number+1) numbers FROM numbers(10)
```

Result:

```text
┌─numbers─┐
│ [9,10]  │
└─────────┘
```

In compare to `groupArray`:

```sql
SELECT groupArray(2)(number+1) numbers FROM numbers(10)
```

```text
┌─numbers─┐
│ [1,2]   │
└─────────┘
```
