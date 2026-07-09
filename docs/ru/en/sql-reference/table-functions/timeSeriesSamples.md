---
description: 'timeSeriesSamples возвращает таблицу samples, используемую таблицей `db_name.time_series_table`
  с движком таблицы TimeSeries.'
sidebar_label: 'timeSeriesSamples'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesSamples
title: 'timeSeriesSamples'
doc_type: 'reference'
---

`timeSeriesSamples(db_name.time_series_table)` — возвращает таблицу [samples](../../engines/table-engines/integrations/time-series.md#samples-table), используемую таблицей `db_name.time_series_table` с [движком таблицы](../../engines/table-engines/integrations/time-series.md) `TimeSeries`:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries SAMPLES samples_table
```

Функция также работает, если таблица *samples* — внутренняя:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries SAMPLES INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

Следующие запросы эквивалентны:

```sql
SELECT * FROM timeSeriesSamples(db_name.time_series_table);
SELECT * FROM timeSeriesSamples('db_name.time_series_table');
SELECT * FROM timeSeriesSamples('db_name', 'time_series_table');
```

:::note
У функции `timeSeriesSamples` есть псевдоним `timeSeriesData`, сохранённый для обратной совместимости.
:::