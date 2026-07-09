---
description: 'A função de tabela timeSeriesTags retorna a tabela de tags usada pela tabela `db_name.time_series_table`
  cujo motor de tabela é o [TimeSeries](../../engines/table-engines/integrations/time-series.md).'
sidebar_label: 'timeSeriesTags'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesTags
title: 'timeSeriesTags'
doc_type: 'reference'
---

`timeSeriesTags(db_name.time_series_table)` - retorna a [tabela de tags](../../engines/table-engines/integrations/time-series.md#tags-table)
usada pela tabela `db_name.time_series_table`, cujo motor de tabela é o [TimeSeries](../../engines/table-engines/integrations/time-series.md):

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS tags_table
```

A função também funciona se *tags* for a tabela interna:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

As consultas a seguir são equivalentes:

```sql
SELECT * FROM timeSeriesTags(db_name.time_series_table);
SELECT * FROM timeSeriesTags('db_name.time_series_table');
SELECT * FROM timeSeriesTags('db_name', 'time_series_table');
```