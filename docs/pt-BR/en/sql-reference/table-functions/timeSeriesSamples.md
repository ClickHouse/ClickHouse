---
description: 'timeSeriesSamples retorna a tabela de samples usada pela tabela `db_name.time_series_table`
  cujo mecanismo de tabela é TimeSeries.'
sidebar_label: 'timeSeriesSamples'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesSamples
title: 'timeSeriesSamples'
doc_type: 'referência'
---

`timeSeriesSamples(db_name.time_series_table)` - Retorna a tabela de [samples](../../engines/table-engines/integrations/time-series.md#samples-table)
usada pela tabela `db_name.time_series_table` cujo mecanismo de tabela é [TimeSeries](../../engines/table-engines/integrations/time-series.md):

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries SAMPLES samples_table
```

A função também funciona caso a tabela *samples* seja interna:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries SAMPLES INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

As consultas a seguir são equivalentes:

```sql
SELECT * FROM timeSeriesSamples(db_name.time_series_table);
SELECT * FROM timeSeriesSamples('db_name.time_series_table');
SELECT * FROM timeSeriesSamples('db_name', 'time_series_table');
```

:::note
A função `timeSeriesSamples` tem o alias `timeSeriesData`, mantido para garantir compatibilidade com versões anteriores.
:::