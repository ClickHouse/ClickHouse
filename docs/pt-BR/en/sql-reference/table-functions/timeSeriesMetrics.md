---
description: 'timeSeriesMetrics retorna a tabela de métricas usada pela tabela `db_name.time_series_table`
  cujo motor de tabela é o motor TimeSeries.'
sidebar_label: 'timeSeriesMetrics'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesMetrics
title: 'timeSeriesMetrics'
doc_type: 'referência'
---

`timeSeriesMetrics(db_name.time_series_table)` - retorna a [tabela de métricas](../../engines/table-engines/integrations/time-series.md#metrics-table)
usada pela tabela `db_name.time_series_table`, cujo motor de tabela é o motor [TimeSeries](../../engines/table-engines/integrations/time-series.md):

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS metrics_table
```

A função também funciona mesmo se a tabela *metrics* for interna:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

As consultas a seguir são equivalentes:

```sql
SELECT * FROM timeSeriesMetrics(db_name.time_series_table);
SELECT * FROM timeSeriesMetrics('db_name.time_series_table');
SELECT * FROM timeSeriesMetrics('db_name', 'time_series_table');
```