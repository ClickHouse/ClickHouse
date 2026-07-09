---
description: 'La función de tabla timeSeriesTags devuelve la tabla de etiquetas usada por la tabla `db_name.time_series_table`
  cuyo motor de tabla es el motor TimeSeries.'
sidebar_label: 'timeSeriesTags'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesTags
title: 'timeSeriesTags'
doc_type: 'reference'
---

`timeSeriesTags(db_name.time_series_table)` - Devuelve la [tabla de etiquetas](../../engines/table-engines/integrations/time-series.md#tags-table)
usada por la tabla `db_name.time_series_table` cuyo motor de tabla es el motor [TimeSeries](../../engines/table-engines/integrations/time-series.md):

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS tags_table
```

La función también funciona cuando la tabla *tags* es interna:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

Las siguientes consultas son equivalentes:

```sql
SELECT * FROM timeSeriesTags(db_name.time_series_table);
SELECT * FROM timeSeriesTags('db_name.time_series_table');
SELECT * FROM timeSeriesTags('db_name', 'time_series_table');
```