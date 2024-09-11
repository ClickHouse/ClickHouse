---
slug: /en/sql-reference/data-types/date32
sidebar_position: 14
sidebar_label: Date32
---

# Date32

A date. Supports the date range same with [DateTime64](../../sql-reference/data-types/datetime64.md). Stored as a signed 32-bit integer in native byte order with the value representing the days since 1970-01-01 (0 represents 1970-01-01 and negative values represent the days before 1970).

**Examples**

Creating a table with a `Date32`-type column and inserting data into it:

``` sql
CREATE TABLE dt32
(
    `timestamp` Date32,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
-- Parse Date
-- - from string,
-- - from 'small' integer interpreted as number of days since 1970-01-01, and
-- - from 'big' integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt32 VALUES ('2100-01-01', 1), (47482, 2), (4102444800, 3);

SELECT * FROM dt32;
```

``` text
┌──timestamp─┬─event_id─┐
│ 2100-01-01 │        1 │
│ 2100-01-01 │        2 │
└────────────┴──────────┘
```

**See Also**

- [toDate32](../../sql-reference/functions/type-conversion-functions.md#todate32)
- [toDate32OrZero](../../sql-reference/functions/type-conversion-functions.md#todate32-or-zero)
- [toDate32OrNull](../../sql-reference/functions/type-conversion-functions.md#todate32-or-null)
