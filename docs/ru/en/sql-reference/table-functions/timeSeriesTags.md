---
description: 'Табличная функция timeSeriesTags возвращает таблицу тегов, используемую таблицей `db_name.time_series_table`,
  у которой в качестве движка таблицы используется TimeSeries.'
sidebar_label: 'timeSeriesTags'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesTags
title: 'timeSeriesTags'
doc_type: 'reference'
---

`timeSeriesTags(db_name.time_series_table)` - Возвращает [таблицу тегов](../../engines/table-engines/integrations/time-series.md#tags-table),
используемую таблицей `db_name.time_series_table`, у которой в качестве движка таблицы используется [TimeSeries](../../engines/table-engines/integrations/time-series.md):

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS tags_table
```

Функция также работает, если таблица *tags* — внутренняя:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

Следующие запросы равнозначны:

```sql
SELECT * FROM timeSeriesTags(db_name.time_series_table);
SELECT * FROM timeSeriesTags('db_name.time_series_table');
SELECT * FROM timeSeriesTags('db_name', 'time_series_table');
```