---
description: 'Lê séries temporais de uma tabela TimeSeries filtradas por um seletor e com timestamps dentro de um intervalo especificado.'
sidebar_label: 'timeSeriesSelector'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesSelector
title: 'timeSeriesSelector'
doc_type: 'reference'
---

Lê séries temporais de uma tabela TimeSeries filtradas por um seletor e com timestamps dentro de um intervalo especificado.
Esta função é semelhante aos [seletores de intervalo](https://prometheus.io/docs/prometheus/latest/querying/basics/#range-vector-selectors), mas também é usada para implementar [seletores instantâneos](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors).

<div id="syntax">
  ## Sintaxe
</div>

```sql
timeSeriesSelector('db_name', 'time_series_table', 'instant_query', min_time, max_time)
timeSeriesSelector(db_name.time_series_table, 'instant_query', min_time, max_time)
timeSeriesSelector('time_series_table', 'instant_query', min_time, max_time)
```

<div id="arguments">
  ## Argumentos
</div>

* `db_name` - O nome do banco de dados onde está localizada uma tabela TimeSeries.
* `time_series_table` - O nome de uma tabela TimeSeries.
* `instant_query` - Um seletor instantâneo escrito em [sintaxe PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors), sem os modificadores `@` ou `offset`.
* &#96;min&#95;time - Timestamp inicial, inclusive.
* &#96;max&#95;time - Timestamp final, inclusive.

<div id="returned_value">
  ## Valor retornado
</div>

A função retorna três colunas:

* `id` - Contém os identificadores das séries temporais que correspondem ao seletor especificado.
* `timestamp` - Contém timestamps.
* `value` - Contém valores.

Os dados retornados não seguem uma ordem específica.

<div id="example">
  ## Exemplo
</div>

```sql
SELECT * FROM timeSeriesSelector(mytable, 'http_requests{job="prometheus"}', now() - INTERVAL 10 MINUTES, now())
```