---
description: 'Avalia uma consulta do Prometheus usando dados de uma tabela TimeSeries.'
sidebar_label: 'prometheusQueryRange'
sidebar_position: 145
slug: /sql-reference/table-functions/prometheusQueryRange
title: 'prometheusQueryRange'
doc_type: 'reference'
---

Avalia uma consulta do Prometheus usando dados de uma tabela TimeSeries ao longo de um intervalo de tempos de avaliação.

<div id="syntax">
  ## Sintaxe
</div>

```sql
prometheusQueryRange('db_name', 'time_series_table', 'promql_query', start_time, end_time, step)
prometheusQueryRange(db_name.time_series_table, 'promql_query', start_time, end_time, step)
prometheusQueryRange('time_series_table', 'promql_query', start_time, end_time, step)
```

<div id="arguments">
  ## Argumentos
</div>

* `db_name` - O nome do banco de dados onde está localizada uma tabela TimeSeries.
* `time_series_table` - O nome de uma tabela TimeSeries.
* `promql_query` - Uma consulta escrita na [sintaxe PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/).
* `start_time` - A hora de início do intervalo de avaliação.
* `end_time` - A hora de término do intervalo de avaliação.
* `step` - O passo usado para avançar o tempo de avaliação de `start_time` até `end_time` (inclusive).

<div id="returned_value">
  ## Valor retornado
</div>

A função pode retornar colunas diferentes, dependendo do tipo de resultado da consulta passada ao parâmetro `promql_query`:

| Tipo de resultado | Colunas de resultado                                                                      | Exemplo                                             |
| ----------------- | ----------------------------------------------------------------------------------------- | --------------------------------------------------- |
| vector            | tags Array(Tuple(String, String)), timestamp TimestampType, value ValueType               | prometheusQuery(mytable, &#39;up&#39;)              |
| matrix            | tags Array(Tuple(String, String)), time&#95;series Array(Tuple(TimestampType, ValueType)) | prometheusQuery(mytable, &#39;up[1m]&#39;)          |
| scalar            | scalar ValueType                                                                          | prometheusQuery(mytable, &#39;1h30m&#39;)           |
| string            | string String                                                                             | prometheusQuery(mytable, &#39;&quot;abc&quot;&#39;) |

<div id="supported-promql-features">
  ## Recursos do PromQL suportados
</div>

<div id="selectors">
  ### Seletores
</div>

Seletores instantâneos, seletores de intervalo, matchers de rótulo (`=`, `!=`, `=~`, `!~`), modificadores `offset`, modificadores `@` de timestamp e subconsultas.

<div id="functions">
  ### Funções
</div>

| Categoria     | Funções                                                                                          |
| ------------- | ------------------------------------------------------------------------------------------------ |
| Intervalo     | `rate`, `irate`, `delta`, `idelta`, `last_over_time`                                             |
| Matemática    | `abs`, `sgn`, `floor`, `ceil`, `sqrt`, `exp`, `ln`, `log2`, `log10`, `rad`, `deg`                |
| Trigonometria | `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh`   |
| DateTime      | `day_of_week`, `day_of_month`, `days_in_month`, `day_of_year`, `minute`, `hour`, `month`, `year` |
| Tipo          | `scalar`, `vector`                                                                               |
| Histograma    | `histogram_quantile`                                                                             |
| Outros        | `time`, `pi`                                                                                     |

**Nota**: `histogram_quantile` usa interpolação linear em buckets de histogramas clássicos (identificados pelo rótulo `le`). Histogramas nativos ainda não são compatíveis, e o argumento `phi` (nível de quantil) no momento deve ser um scalar constante — expressions que variam a cada passo, como `histogram_quantile(time() / 1000, ...)`, são rejeitadas com o error `NOT_IMPLEMENTED`.

<div id="operators">
  ### Operadores
</div>

Todos os operadores binários aritméticos (`+`, `-`, `*`, `/`, `%`, `^`), de comparação (`==`, `!=`, `<`, `>`, `<=`, `>=` com `bool` opcional) e lógicos (`and`, `or`, `unless`), com os modificadores `on()`/`ignoring()` e `group_left()`/`group_right()`.

Operadores unários `+` e `-`.

<div id="aggregation-operators">
  ### Operadores de agregação
</div>

`sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `group`, `quantile`, `topk`, `bottomk`, `limitk` — com os modificadores opcionais `by()` ou `without()`.

Ainda não há suporte para `count_values`.

<div id="example">
  ## Exemplo
</div>

```sql
SELECT * FROM prometheusQueryRange(mytable, 'rate(http_requests{job="prometheus"}[10m])[1h:10m]', now() - INTERVAL 10 MINUTES, now(), INTERVAL 1 MINUTE)
```