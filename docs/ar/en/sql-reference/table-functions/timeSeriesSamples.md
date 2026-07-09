---
description: 'تعيد timeSeriesSamples جدول العينات الذي يستخدمه الجدول `db_name.time_series_table`
  والذي يكون محرك الجدول له هو TimeSeries.'
sidebar_label: 'timeSeriesSamples'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesSamples
title: 'timeSeriesSamples'
doc_type: 'مرجع'
---

`timeSeriesSamples(db_name.time_series_table)` - تعيد [جدول العينات](../../engines/table-engines/integrations/time-series.md#samples-table) الذي
يستخدمه الجدول `db_name.time_series_table` والذي يكون [محرك الجدول](../../engines/table-engines/integrations/time-series.md) له هو TimeSeries:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries SAMPLES samples_table
```

تعمل الدالة أيضًا إذا كان جدول *samples* داخليًا:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries SAMPLES INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

الاستعلامات التالية متكافئة:

```sql
SELECT * FROM timeSeriesSamples(db_name.time_series_table);
SELECT * FROM timeSeriesSamples('db_name.time_series_table');
SELECT * FROM timeSeriesSamples('db_name', 'time_series_table');
```

:::note
تحتفظ الدالة `timeSeriesSamples` بالاسم المستعار `timeSeriesData` حفاظًا على التوافق مع الإصدارات السابقة.
:::