---
slug: /en/sql-reference/data-types/ddsketch
sidebar_position: 65
sidebar_label: DDSketch(relative_error)
---

# DDSketch(relative_error)

DDSketch, a quantile sketch with relative-error guarantees. This sketch computes quantile values with an approximation error that is relative to the actual quantile value. It works on both negative and non-negative input values.

For instance, using DDSketch with a relative accuracy guarantee set to 1%, if the expected quantile value is 100, the computed quantile value is guaranteed to be between 99 and 101. If the expected quantile value is 1000, the computed quantile value is guaranteed to be between 990 and 1010.
For more information on ddsketch, see [computing accurate percentiles with ddsketch](https://www.datadoghq.com/blog/engineering/computing-accurate-percentiles-with-ddsketch/).

**Parameters**

-   `relative_error` — a Float32 type value which must be between 0 and 1

**Examples**

Creating a table with a `DDSketch` column and inserting data into it:

``` sql
CREATE TABLE NetTrafficMonitor(`timestamp` DateTime64(3, 'Asia'), `sendLatency` Float64) ENGINE = Memory;
CREATE TABLE TestSketch(`name` String, `timestamp` DateTime64(3, 'Asia'), `sketch` NULLABLE(DDSketch(0.01))) ENGINE = Memory;

WITH thetime = toDateTime64('2022-01-01 10:20:30.999', 3);
INSERT INTO TestSketch
    SELECT 'sendLatency', thetime, ddsketchBuild(0.01, sendLatency) FROM NetTrafficMonitor WHERE timestamp < thetime;

WITH thetime = toDateTime64('2020-01-01 10:20:30.999', 3);
INSERT INTO TestSketch
    SELECT 'sendLatency', thetime, ddsketchBuildIf(0.01, sendLatency, timestamp < thetime) FROM NetTrafficMonitor;
```
## Related Content

- [Getting Data Into ClickHouse - Part 2 - A JSON detour](https://clickhouse.com/blog/getting-data-into-clickhouse-part-2-json)
