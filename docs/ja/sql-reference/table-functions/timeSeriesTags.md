---
slug: /ja/sql-reference/table-functions/timeSeriesTags
sidebar_position: 145
sidebar_label: timeSeriesTags
---

# timeSeriesTags

`timeSeriesTags(db_name.time_series_table)` - テーブルエンジンが [TimeSeries](../../engines/table-engines/integrations/time-series.md) のテーブル `db_name.time_series_table` で使用されている[タグ](../../engines/table-engines/integrations/time-series.md#tags-table)テーブルを返します:

``` sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS tags_table
```

この関数は、_tags_ テーブルが内部にある場合でも機能します:

``` sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

以下のクエリは同等です:

``` sql
SELECT * FROM timeSeriesTags(db_name.time_series_table);
SELECT * FROM timeSeriesTags('db_name.time_series_table');
SELECT * FROM timeSeriesTags('db_name', 'time_series_table');
```
