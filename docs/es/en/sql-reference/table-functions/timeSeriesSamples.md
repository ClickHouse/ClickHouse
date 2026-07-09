---
description: 'timeSeriesSamples devuelve la tabla de muestras utilizada por la tabla `db_name.time_series_table`
  cuyo motor de tabla es TimeSeries.'
sidebar_label: 'timeSeriesSamples'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesSamples
title: 'timeSeriesSamples'
doc_type: 'reference'
---

`timeSeriesSamples(db_name.time_series_table)` - Devuelve la tabla de [muestras](../../engines/table-engines/integrations/time-series.md#samples-table)
utilizada por la tabla `db_name.time_series_table` cuyo motor de tabla es [TimeSeries](../../engines/table-engines/integrations/time-series.md):

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries SAMPLES samples_table
```

La función también se puede usar si la tabla *samples* es interna:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries SAMPLES INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

Las siguientes consultas son equivalentes:

```sql
SELECT * FROM timeSeriesSamples(db_name.time_series_table);
SELECT * FROM timeSeriesSamples('db_name.time_series_table');
SELECT * FROM timeSeriesSamples('db_name', 'time_series_table');
```

:::note
La función `timeSeriesSamples` tiene el alias `timeSeriesData`, que se conserva para mantener la compatibilidad con versiones anteriores.
:::