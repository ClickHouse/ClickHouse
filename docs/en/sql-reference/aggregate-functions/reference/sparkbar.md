---
slug: /en/sql-reference/aggregate-functions/reference/sparkbar
sidebar_position: 187
sidebar_label: sparkbar
---

# sparkbar

The function plots a frequency histogram for values `x` and the repetition rate `y` of these values over the interval `[min_x, max_x]`.
Repetitions for all `x` falling into the same bucket are averaged, so data should be pre-aggregated.
Negative repetitions are ignored.

If no interval is specified, then the minimum `x` is used as the interval start, and the maximum `x` — as the interval end.
Otherwise, values outside the interval are ignored.

**Syntax**

``` sql
sparkbar(buckets[, min_x, max_x])(x, y)
```

**Parameters**

- `buckets` — The number of segments. Type: [Integer](../../../sql-reference/data-types/int-uint.md).
- `min_x` — The interval start. Optional parameter.
- `max_x` — The interval end. Optional parameter.

**Arguments**

- `x` — The field with values.
- `y` — The field with the frequency of values.

**Returned value**

- The frequency histogram.

**Example**

Query:

``` sql
CREATE TABLE spark_bar_data (`value` Int64, `event_date` Date) ENGINE = MergeTree ORDER BY event_date;

INSERT INTO spark_bar_data VALUES (1,'2020-01-01'), (3,'2020-01-02'), (4,'2020-01-02'), (-3,'2020-01-02'), (5,'2020-01-03'), (2,'2020-01-04'), (3,'2020-01-05'), (7,'2020-01-06'), (6,'2020-01-07'), (8,'2020-01-08'), (2,'2020-01-11');

SELECT sparkbar(9)(event_date,cnt) FROM (SELECT sum(value) as cnt, event_date FROM spark_bar_data GROUP BY event_date);

SELECT sparkbar(9, toDate('2020-01-01'), toDate('2020-01-10'))(event_date,cnt) FROM (SELECT sum(value) as cnt, event_date FROM spark_bar_data GROUP BY event_date);
```

Result:

``` text
┌─sparkbar(9)(event_date, cnt)─┐
│ ▂▅▂▃▆█  ▂                    │
└──────────────────────────────┘

┌─sparkbar(9, toDate('2020-01-01'), toDate('2020-01-10'))(event_date, cnt)─┐
│ ▂▅▂▃▇▆█                                                                  │
└──────────────────────────────────────────────────────────────────────────┘
```

The alias for this function is sparkBar.

