---
description: 'timeSeriesTags table function returns the tags table use by table `db_name.time_series_table`
  whose table engine is the TimeSeries engine.'
sidebar_label: 'timeSeriesTags'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesTags
title: 'timeSeriesTags'
---

# timeSeriesTags Table Function

`timeSeriesTags(db_name.time_series_table)` - Returns the [tags](../../engines/table-engines/integrations/time-series.md#tags-table) table
used by table `db_name.time_series_table` whose table engine is the [TimeSeries](../../engines/table-engines/integrations/time-series.md) engine:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS tags_table
```

The function also works if the _tags_ table is inner:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

The following queries are equivalent:

```sql
SELECT * FROM timeSeriesTags(db_name.time_series_table);
SELECT * FROM timeSeriesTags('db_name.time_series_table');
SELECT * FROM timeSeriesTags('db_name', 'time_series_table');
```
