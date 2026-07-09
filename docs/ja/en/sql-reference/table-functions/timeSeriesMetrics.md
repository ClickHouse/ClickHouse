---
description: '`timeSeriesMetrics` は、テーブルエンジンが TimeSeries エンジンであるテーブル `db_name.time_series_table`
  で使用される Metrics テーブルを返します。'
sidebar_label: 'timeSeriesMetrics'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesMetrics
title: 'timeSeriesMetrics'
doc_type: 'reference'
---

`timeSeriesMetrics(db_name.time_series_table)` - テーブルエンジンが [TimeSeries](../../engines/table-engines/integrations/time-series.md) エンジンであるテーブル `db_name.time_series_table` で使用される [Metrics テーブル](../../engines/table-engines/integrations/time-series.md#metrics-table) を返します:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS metrics_table
```

この関数は、*metrics*テーブルが内側のテーブルである場合にも機能します。

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

以下のクエリは同等です。

```sql
SELECT * FROM timeSeriesMetrics(db_name.time_series_table);
SELECT * FROM timeSeriesMetrics('db_name.time_series_table');
SELECT * FROM timeSeriesMetrics('db_name', 'time_series_table');
```