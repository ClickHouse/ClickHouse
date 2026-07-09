---
description: 'تعيد دالة الجدول timeSeriesTags جدول الوسوم الذي يستخدمه الجدول `db_name.time_series_table`
  والذي يكون محرك جدوله هو المحرك TimeSeries.'
sidebar_label: 'timeSeriesTags'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesTags
title: 'timeSeriesTags'
doc_type: 'مرجع'
---

`timeSeriesTags(db_name.time_series_table)` - تعيد [جدول الوسوم](../../engines/table-engines/integrations/time-series.md#tags-table)
الذي يستخدمه الجدول `db_name.time_series_table` والذي يكون محرك جدوله هو المحرك [TimeSeries](../../engines/table-engines/integrations/time-series.md):

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS tags_table
```

تعمل الدالة أيضًا إذا كان جدول *tags* جدولًا داخليًا:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

الاستعلامات التالية متساوية:

```sql
SELECT * FROM timeSeriesTags(db_name.time_series_table);
SELECT * FROM timeSeriesTags('db_name.time_series_table');
SELECT * FROM timeSeriesTags('db_name', 'time_series_table');
```