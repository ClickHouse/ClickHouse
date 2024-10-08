---
slug: /en/sql-reference/table-functions/timeSeriesMetrics
sidebar_position: 145
sidebar_label: timeSeriesMetrics
---

# timeSeriesMetrics

`timeSeriesMetrics(db_name.time_series_table)` - Returns the [metrics](../../engines/table-engines/integrations/time-series.md#metrics-table) table
used by table `db_name.time_series_table` which table engine is [TimeSeries](../../engines/table-engines/integrations/time-series.md):

``` sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS metrics_table
```

The function also works if the _metrics_ table is inner:

``` sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

The following queries are equivalent:

``` sql
SELECT * FROM timeSeriesMetrics(db_name.time_series_table);
SELECT * FROM timeSeriesMetrics('db_name.time_series_table');
SELECT * FROM timeSeriesMetrics('db_name', 'time_series_table');
```
