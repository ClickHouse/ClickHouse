---
slug: /ja/sql-reference/table-functions/timeSeriesMetrics
sidebar_position: 145
sidebar_label: timeSeriesMetrics
---

# timeSeriesMetrics

`timeSeriesMetrics(db_name.time_series_table)` - [TimeSeries](../../engines/table-engines/integrations/time-series.md)テーブルエンジンを使用するテーブル`db_name.time_series_table`で使用される[metrics](../../engines/table-engines/integrations/time-series.md#metrics-table)テーブルを返します。

``` sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS metrics_table
```

_metrics_テーブルが内側にある場合もこの関数は機能します。

``` sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

以下のクエリは同等です。

``` sql
SELECT * FROM timeSeriesMetrics(db_name.time_series_table);
SELECT * FROM timeSeriesMetrics('db_name.time_series_table');
SELECT * FROM timeSeriesMetrics('db_name', 'time_series_table');
```

