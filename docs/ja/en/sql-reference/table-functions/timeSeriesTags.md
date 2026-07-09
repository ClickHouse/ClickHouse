---
description: 'テーブルエンジンが TimeSeries エンジンである `db_name.time_series_table` テーブルで使用される [Tags テーブル](../../engines/table-engines/integrations/time-series.md#tags-table) を返す `timeSeriesTags` テーブル関数。'
sidebar_label: 'timeSeriesTags'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesTags
title: 'timeSeriesTags'
doc_type: 'reference'
---

`timeSeriesTags(db_name.time_series_table)` - テーブルエンジンが [TimeSeries](../../engines/table-engines/integrations/time-series.md) エンジンである `db_name.time_series_table` テーブルで使用される [Tags テーブル](../../engines/table-engines/integrations/time-series.md#tags-table) を返します:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS tags_table
```

この関数は、*tags* テーブルが inner の場合でも機能します:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

次のクエリは同等です：

```sql
SELECT * FROM timeSeriesTags(db_name.time_series_table);
SELECT * FROM timeSeriesTags('db_name.time_series_table');
SELECT * FROM timeSeriesTags('db_name', 'time_series_table');
```