---
slug: /en/sql-reference/aggregate-functions/reference/least
title: least
---

Aggregate function that returns the least across a list of values.  All of the list members must be of comparable types.

Examples:

```sql
SELECT
    toTypeName(least(toUInt8(1), 2, toUInt8(3), 3.)),
    least(1, 2, toUInt8(3), 3.)
```
```response
┌─toTypeName(least(toUInt8(1), 2, toUInt8(3), 3.))─┬─least(1, 2, toUInt8(3), 3.)─┐
│ Float64                                          │                           1 │
└──────────────────────────────────────────────────┴─────────────────────────────┘
```

:::note
The type returned is a Float64 as the UInt8 must be promoted to 64 bit for the comparison.
:::

```sql
SELECT least(['hello'], ['there'], ['world'])
```
```response
┌─least(['hello'], ['there'], ['world'])─┐
│ ['hello']                              │
└────────────────────────────────────────┘
```

```sql
SELECT least(toDateTime32(now() + toIntervalDay(1)), toDateTime64(now(), 3))
```
```response
┌─least(toDateTime32(plus(now(), toIntervalDay(1))), toDateTime64(now(), 3))─┐
│                                                    2023-05-12 01:16:59.000 │
└────────────────────────────────────────────────────────────────────────────┘
```

:::note
The type returned is a DateTime64 as the DataTime32 must be promoted to 64 bit for the comparison.
:::

Also see [greatest](/docs/en/sql-reference/aggregate-functions/reference/greatest.md).

