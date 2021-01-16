---
toc_priority: 170
---

# timeSeriesGroupSum {#agg-function-timeseriesgroupsum}

Syntax: `timeSeriesGroupSum(uid, timestamp, value)`

`timeSeriesGroupSum` can aggregate different time series that sample timestamp not alignment.
It will use linear interpolation between two sample timestamp and then sum time-series together.

-   `uid` is the time series unique id, `UInt64`.
-   `timestamp` is Int64 type in order to support millisecond or microsecond.
-   `value` is the metric.

The function returns array of tuples with `(timestamp, aggregated_value)` pairs.

Before using this function make sure `timestamp` is in ascending order.

Example:

``` text
┌─uid─┬─timestamp─┬─value─┐
│ 1   │     2     │   0.2 │
│ 1   │     7     │   0.7 │
│ 1   │    12     │   1.2 │
│ 1   │    17     │   1.7 │
│ 1   │    25     │   2.5 │
│ 2   │     3     │   0.6 │
│ 2   │     8     │   1.6 │
│ 2   │    12     │   2.4 │
│ 2   │    18     │   3.6 │
│ 2   │    24     │   4.8 │
└─────┴───────────┴───────┘
```

``` sql
CREATE TABLE time_series(
    uid       UInt64,
    timestamp Int64,
    value     Float64
) ENGINE = Memory;
INSERT INTO time_series VALUES
    (1,2,0.2),(1,7,0.7),(1,12,1.2),(1,17,1.7),(1,25,2.5),
    (2,3,0.6),(2,8,1.6),(2,12,2.4),(2,18,3.6),(2,24,4.8);

SELECT timeSeriesGroupSum(uid, timestamp, value)
FROM (
    SELECT * FROM time_series order by timestamp ASC
);
```

And the result will be:

``` text
[(2,0.2),(3,0.9),(7,2.1),(8,2.4),(12,3.6),(17,5.1),(18,5.4),(24,7.2),(25,2.5)]
```
