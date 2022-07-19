---
toc_priority: 48
toc_title: Date32
---

# Date32 {#data_type-datetime32}

A date. Supports the date range same with [Datetime64](../../sql-reference/data-types/datetime64.md). Stored in four bytes as the number of days since 1925-01-01. Allows storing values till 2283-11-11. 

**Examples**

Creating a table with a `Date32`-type column and inserting data into it:

``` sql
CREATE TABLE new
(
    `timestamp` Date32,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
INSERT INTO new VALUES (4102444800, 1), ('2100-01-01', 2);
SELECT * FROM new;
```

``` text
┌──timestamp─┬─event_id─┐
│ 2100-01-01 │        1 │
│ 2100-01-01 │        2 │
└────────────┴──────────┘
```

**See Also**

-   [toDate32](../../sql-reference/functions/type-conversion-functions.md#todate32)
-   [toDate32OrZero](../../sql-reference/functions/type-conversion-functions.md#todate32-or-zero)
-   [toDate32OrNull](../../sql-reference/functions/type-conversion-functions.md#todate32-or-null) 

