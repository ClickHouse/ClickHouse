---
slug: /en/sql-reference/table-functions/timeSeriesTags
sidebar_position: 145
sidebar_label: timeSeriesTags
---

# timeSeriesTags

`timeSeriesTags(db_name.time_series_table)` - Returns the [tags](../../engines/table-engines/integrations/time-series.md#tags-table) table
used by table `db_name.time_series_table` which table engine is [TimeSeries](../../engines/table-engines/integrations/time-series.md):

``` sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS tags_table
```

The function also works if the _tags_ table is inner:

``` sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

The following queries are equivalent:

``` sql
SELECT * FROM timeSeriesTags(db_name.time_series_table);
SELECT * FROM timeSeriesTags('db_name.time_series_table');
SELECT * FROM timeSeriesTags('db_name', 'time_series_table');
```
