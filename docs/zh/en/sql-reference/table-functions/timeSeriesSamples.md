---
description: '`timeSeriesSamples` 返回表引擎为 TimeSeries 的表 `db_name.time_series_table` 所使用的 samples 表。'
sidebar_label: 'timeSeriesSamples'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesSamples
title: 'timeSeriesSamples'
doc_type: '参考'
---

`timeSeriesSamples(db_name.time_series_table)` - 返回表引擎为 [TimeSeries](../../engines/table-engines/integrations/time-series.md) 的表 `db_name.time_series_table` 所使用的 [samples](../../engines/table-engines/integrations/time-series.md#samples-table) 表：

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries SAMPLES samples_table
```

即使 *samples* 表是内表，该函数同样适用：

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries SAMPLES INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

以下查询等价：

```sql
SELECT * FROM timeSeriesSamples(db_name.time_series_table);
SELECT * FROM timeSeriesSamples('db_name.time_series_table');
SELECT * FROM timeSeriesSamples('db_name', 'time_series_table');
```

:::note
函数 `timeSeriesSamples` 有一个别名 `timeSeriesData`，保留该别名是为了向后兼容。
:::