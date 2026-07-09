---
description: 'تعيد `timeSeriesMetrics` جدول المقاييس الذي يستخدمه الجدول `db_name.time_series_table`
  والذي يكون محرك جدوله من نوع TimeSeries.'
sidebar_label: 'timeSeriesMetrics'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesMetrics
title: 'timeSeriesMetrics'
doc_type: 'reference'
---

`timeSeriesMetrics(db_name.time_series_table)` - تعيد جدول [المقاييس](../../engines/table-engines/integrations/time-series.md#metrics-table)
الذي يستخدمه الجدول `db_name.time_series_table` والذي يكون محرك جدوله من نوع [TimeSeries](../../engines/table-engines/integrations/time-series.md):

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS metrics_table
```

تعمل الدالة أيضًا إذا كان جدول *المقاييس* داخليًا:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

الاستعلامات التالية متكافئة:

```sql
SELECT * FROM timeSeriesMetrics(db_name.time_series_table);
SELECT * FROM timeSeriesMetrics('db_name.time_series_table');
SELECT * FROM timeSeriesMetrics('db_name', 'time_series_table');
```