---
description: 'timeSeriesSamples は、テーブルエンジンが TimeSeries であるテーブル `db_name.time_series_table`
  で使用される Samples テーブルを返します。'
sidebar_label: 'timeSeriesSamples'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesSamples
title: 'timeSeriesSamples'
doc_type: 'reference'
---

`timeSeriesSamples(db_name.time_series_table)` - テーブルエンジンが [TimeSeries](../../engines/table-engines/integrations/time-series.md) であるテーブル `db_name.time_series_table` で使用される [Samples](../../engines/table-engines/integrations/time-series.md#samples-table) テーブルを返します:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries SAMPLES samples_table
```

*samples* テーブルが内側にある場合でも、この関数は動作します：

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries SAMPLES INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

以下のクエリは同等です。

```sql
SELECT * FROM timeSeriesSamples(db_name.time_series_table);
SELECT * FROM timeSeriesSamples('db_name.time_series_table');
SELECT * FROM timeSeriesSamples('db_name', 'time_series_table');
```

:::note
関数 `timeSeriesSamples` には `timeSeriesData` というエイリアスがあり、後方互換性のために維持されています。
:::