---
slug: /en/sql-reference/table-functions/timeSeriesData
sidebar_position: 145
sidebar_label: timeSeriesData
---

# timeSeriesData

`timeSeriesData(db_name.time_series_table)` - Returns the [data](../../engines/table-engines/integrations/time-series.md#data-table) table
used by table `db_name.time_series_table` which table engine is [TimeSeries](../../engines/table-engines/integrations/time-series.md):

``` sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries DATA data_table
```

The function also works if the _data_ table is inner:

``` sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries DATA INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

The following queries are equivalent:

``` sql
SELECT * FROM timeSeriesData(db_name.time_series_table);
SELECT * FROM timeSeriesData('db_name.time_series_table');
SELECT * FROM timeSeriesData('db_name', 'time_series_table');
```
