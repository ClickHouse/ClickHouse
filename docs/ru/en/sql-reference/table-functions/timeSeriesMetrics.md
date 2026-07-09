---
description: 'timeSeriesMetrics возвращает таблицу метрик, которую использует таблица `db_name.time_series_table`,
  у которой в качестве движка таблицы используется TimeSeries.'
sidebar_label: 'timeSeriesMetrics'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesMetrics
title: 'timeSeriesMetrics'
doc_type: 'reference'
---

`timeSeriesMetrics(db_name.time_series_table)` — возвращает [таблицу метрик](../../engines/table-engines/integrations/time-series.md#metrics-table),
которую использует таблица `db_name.time_series_table`, у которой в качестве [движка таблицы](../../engines/table-engines/integrations/time-series.md) используется TimeSeries:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS metrics_table
```

Функция также работает, если таблица *metrics* внутренняя:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

Следующие запросы эквивалентны:

```sql
SELECT * FROM timeSeriesMetrics(db_name.time_series_table);
SELECT * FROM timeSeriesMetrics('db_name.time_series_table');
SELECT * FROM timeSeriesMetrics('db_name', 'time_series_table');
```