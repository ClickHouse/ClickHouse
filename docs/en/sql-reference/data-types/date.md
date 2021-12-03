---
toc_priority: 47
toc_title: Date
---

# Date {#data_type-date}

A date. Stored in two bytes as the number of days since 1970-01-01 (unsigned). Allows storing values from just after the beginning of the Unix Epoch to the upper threshold defined by a constant at the compilation stage (currently, this is until the year 2106, but the final fully-supported year is 2105).

The date value is stored without the time zone.

## Examples {#examples}

**1.** Creating a table with a `DateTime`-type column and inserting data into it:

``` sql
CREATE TABLE dt
(
    `timestamp` Date,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
INSERT INTO dt Values (1546300800, 1), ('2019-01-01', 2);
```

``` sql
SELECT * FROM dt;
```

``` text
┌──timestamp─┬─event_id─┐
│ 2019-01-01 │        1 │
│ 2019-01-01 │        2 │
└────────────┴──────────┘
```

## See Also {#see-also}

-   [Functions for working with dates and times](../../sql-reference/functions/date-time-functions.md)
-   [Operators for working with dates and times](../../sql-reference/operators/index.md#operators-datetime)
-   [`DateTime` data type](../../sql-reference/data-types/datetime.md)


[Original article](https://clickhouse.tech/docs/en/data_types/date/) <!--hide-->
