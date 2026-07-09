---
description: 'Lee series temporales de una tabla TimeSeries, filtradas por un selector y con marcas de tiempo dentro de un intervalo especificado.'
sidebar_label: 'timeSeriesSelector'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesSelector
title: 'timeSeriesSelector'
doc_type: 'reference'
---

Lee series temporales de una tabla TimeSeries, filtradas por un selector y con marcas de tiempo dentro de un intervalo especificado.
Esta función es similar a los [selectores de rango](https://prometheus.io/docs/prometheus/latest/querying/basics/#range-vector-selectors), pero también se usa para implementar [selectores instantáneos](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors).

<div id="syntax">
  ## Sintaxis
</div>

```sql
timeSeriesSelector('db_name', 'time_series_table', 'instant_query', min_time, max_time)
timeSeriesSelector(db_name.time_series_table, 'instant_query', min_time, max_time)
timeSeriesSelector('time_series_table', 'instant_query', min_time, max_time)
```

<div id="arguments">
  ## Argumentos
</div>

* `db_name` - El nombre de la base de datos donde se encuentra la tabla TimeSeries.
* `time_series_table` - El nombre de una tabla TimeSeries.
* `instant_query` - Un selector instantáneo escrito en [sintaxis PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors), sin los modificadores `@` ni `offset`.
* &#96;min&#95;time - marca de tiempo de inicio, inclusiva.
* &#96;max&#95;time - marca de tiempo de fin, inclusiva.

<div id="returned_value">
  ## Valor devuelto
</div>

La función devuelve tres columnas:

* `id` - Contiene los identificadores de las series temporales que coinciden con el selector especificado.
* `timestamp` - Contiene marcas de tiempo.
* `value` - Contiene valores.

Los datos devueltos no siguen ningún orden específico.

<div id="example">
  ## Ejemplo
</div>

```sql
SELECT * FROM timeSeriesSelector(mytable, 'http_requests{job="prometheus"}', now() - INTERVAL 10 MINUTES, now())
```