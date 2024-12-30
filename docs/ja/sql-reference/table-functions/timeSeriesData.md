---
slug: /ja/sql-reference/table-functions/timeSeriesData
sidebar_position: 145
sidebar_label: timeSeriesData
---

# timeSeriesData

`timeSeriesData(db_name.time_series_table)` - [TimeSeries](../../engines/table-engines/integrations/time-series.md) テーブルエンジンを使用するテーブル `db_name.time_series_table` で使用される [data](../../engines/table-engines/integrations/time-series.md#data-table) テーブルを返します：

``` sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries DATA data_table
```

データテーブルが内部である場合もこの関数は動作します：

``` sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries DATA INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

以下のクエリは同等です：

``` sql
SELECT * FROM timeSeriesData(db_name.time_series_table);
SELECT * FROM timeSeriesData('db_name.time_series_table');
SELECT * FROM timeSeriesData('db_name', 'time_series_table');
```
