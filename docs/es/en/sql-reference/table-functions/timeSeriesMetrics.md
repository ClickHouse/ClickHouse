---
description: 'timeSeriesMetrics devuelve la tabla de métricas que usa la tabla `db_name.time_series_table`
  cuyo motor de tabla es el motor TimeSeries.'
sidebar_label: 'timeSeriesMetrics'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesMetrics
title: 'timeSeriesMetrics'
doc_type: 'referencia'
---

`timeSeriesMetrics(db_name.time_series_table)` - Devuelve la [tabla de métricas](../../engines/table-engines/integrations/time-series.md#metrics-table) que usa la tabla `db_name.time_series_table`
cuyo motor de tabla es el motor [TimeSeries](../../engines/table-engines/integrations/time-series.md):

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS metrics_table
```

La función también funciona si la tabla *metrics* es interna:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

Las siguientes consultas son equivalentes:

```sql
SELECT * FROM timeSeriesMetrics(db_name.time_series_table);
SELECT * FROM timeSeriesMetrics('db_name.time_series_table');
SELECT * FROM timeSeriesMetrics('db_name', 'time_series_table');
```