---
slug: /en/sql-reference/aggregate-functions/reference/greatest
title: greatest
---

Aggregate function that returns the greatest across a list of values.  All of the list members must be of comparable types.

Examples:

```sql
SELECT
    toTypeName(greatest(toUInt8(1), 2, toUInt8(3), 3.)),
    greatest(1, 2, toUInt8(3), 3.)
```
```response
┌─toTypeName(greatest(toUInt8(1), 2, toUInt8(3), 3.))─┬─greatest(1, 2, toUInt8(3), 3.)─┐
│ Float64                                             │                              3 │
└─────────────────────────────────────────────────────┴────────────────────────────────┘
```

:::note
The type returned is a Float64 as the UInt8 must be promoted to 64 bit for the comparison.
:::

```sql
SELECT greatest(['hello'], ['there'], ['world'])
```
```response
┌─greatest(['hello'], ['there'], ['world'])─┐
│ ['world']                                 │
└───────────────────────────────────────────┘
```

```sql
SELECT greatest(toDateTime32(now() + toIntervalDay(1)), toDateTime64(now(), 3))
```
```response
┌─greatest(toDateTime32(plus(now(), toIntervalDay(1))), toDateTime64(now(), 3))─┐
│                                                    2023-05-12 01:16:59.000 │
└────────────────────────────────────────────────────────────────────────────┘
```

:::note
The type returned is a DateTime64 as the DataTime32 must be promoted to 64 bit for the comparison.
:::

Also see [least](/docs/en/sql-reference/aggregate-functions/reference/least.md).

