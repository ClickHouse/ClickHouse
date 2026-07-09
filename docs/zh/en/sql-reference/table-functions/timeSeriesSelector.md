---
description: '从 TimeSeries 表中读取经选择器过滤且时间戳位于指定时间间隔内的时间序列。'
sidebar_label: 'timeSeriesSelector'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesSelector
title: 'timeSeriesSelector'
doc_type: 'reference'
---

从 TimeSeries 表中读取经选择器过滤且时间戳位于指定时间间隔内的时间序列。
该函数类似于 [range selectors](https://prometheus.io/docs/prometheus/latest/querying/basics/#range-vector-selectors)，但也可用于实现[即时选择器](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors)。

<div id="syntax">
  ## 语法
</div>

```sql
timeSeriesSelector('db_name', 'time_series_table', 'instant_query', min_time, max_time)
timeSeriesSelector(db_name.time_series_table, 'instant_query', min_time, max_time)
timeSeriesSelector('time_series_table', 'instant_query', min_time, max_time)
```

<div id="arguments">
  ## 参数
</div>

* `db_name` - TimeSeries 表所在的数据库名称。
* `time_series_table` - TimeSeries 表名称。
* `instant_query` - 使用 [PromQL 语法](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors)编写的即时选择器，不包含 `@` 或 `offset` 修饰符。
* &#96;min&#95;time - 起始时间戳，包含该时间点。
* &#96;max&#95;time - 结束时间戳，包含该时间点。

<div id="returned_value">
  ## 返回值
</div>

该函数返回三列：

* `id` - 包含与指定选择器匹配的时间序列标识符。
* `timestamp` - 包含时间戳。
* `value` - 包含值。

返回的数据顺序不固定。

<div id="example">
  ## 示例
</div>

```sql
SELECT * FROM timeSeriesSelector(mytable, 'http_requests{job="prometheus"}', now() - INTERVAL 10 MINUTES, now())
```