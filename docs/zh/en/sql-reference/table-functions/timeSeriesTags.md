---
description: 'timeSeriesTags 表函数返回表 `db_name.time_series_table`
  所使用的标签表，该表的表引擎为 TimeSeries 引擎。'
sidebar_label: 'timeSeriesTags'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesTags
title: 'timeSeriesTags'
doc_type: 'reference'
---

`timeSeriesTags(db_name.time_series_table)` - 返回表 `db_name.time_series_table`
所使用的 [标签](../../engines/table-engines/integrations/time-series.md#tags-table) 表，该表的表引擎为 [TimeSeries](../../engines/table-engines/integrations/time-series.md) 引擎：

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS tags_table
```

如果 *tags* 表是 inner 表，该函数也同样适用：

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

以下查询是等价的：

```sql
SELECT * FROM timeSeriesTags(db_name.time_series_table);
SELECT * FROM timeSeriesTags('db_name.time_series_table');
SELECT * FROM timeSeriesTags('db_name', 'time_series_table');
```