---
description: 'Вычисляет запрос Prometheus на основе данных из таблицы TimeSeries.'
sidebar_label: 'prometheusQueryRange'
sidebar_position: 145
slug: /sql-reference/table-functions/prometheusQueryRange
title: 'prometheusQueryRange'
doc_type: 'reference'
---

Вычисляет запрос Prometheus на основе данных из таблицы TimeSeries для диапазона моментов вычисления.

<div id="syntax">
  ## Синтаксис
</div>

```sql
prometheusQueryRange('db_name', 'time_series_table', 'promql_query', start_time, end_time, step)
prometheusQueryRange(db_name.time_series_table, 'promql_query', start_time, end_time, step)
prometheusQueryRange('time_series_table', 'promql_query', start_time, end_time, step)
```

<div id="arguments">
  ## Аргументы
</div>

* `db_name` - Имя базы данных, в которой находится таблица TimeSeries.
* `time_series_table` - Имя таблицы TimeSeries.
* `promql_query` - Запрос, написанный на [языке PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/).
* `start_time` - Время начала интервала вычисления.
* `end_time` - Время окончания интервала вычисления.
* `step` - Шаг, с которым перебирается время вычисления от `start_time` до `end_time` (включительно).

<div id="returned_value">
  ## Возвращаемое значение
</div>

Функция может возвращать разные столбцы в зависимости от типа результата запроса, переданного в параметр `promql_query`:

| Тип результата | Столбцы результата                                                                        | Пример                                              |
| -------------- | ----------------------------------------------------------------------------------------- | --------------------------------------------------- |
| vector         | tags Array(Tuple(String, String)), timestamp TimestampType, value ValueType               | prometheusQuery(mytable, &#39;up&#39;)              |
| matrix         | tags Array(Tuple(String, String)), time&#95;series Array(Tuple(TimestampType, ValueType)) | prometheusQuery(mytable, &#39;up[1m]&#39;)          |
| scalar         | scalar ValueType                                                                          | prometheusQuery(mytable, &#39;1h30m&#39;)           |
| string         | string String                                                                             | prometheusQuery(mytable, &#39;&quot;abc&quot;&#39;) |

<div id="supported-promql-features">
  ## Поддерживаемые возможности PromQL
</div>

<div id="selectors">
  ### Селекторы
</div>

Мгновенные селекторы, селекторы диапазона, сопоставители меток (`=`, `!=`, `=~`, `!~`), модификаторы offset, модификаторы временной метки `@` и подзапросы.

<div id="functions">
  ### Функции
</div>

| Категория     | Функции                                                                                          |
| ------------- | ------------------------------------------------------------------------------------------------ |
| Диапазон      | `rate`, `irate`, `delta`, `idelta`, `last_over_time`                                             |
| Математика    | `abs`, `sgn`, `floor`, `ceil`, `sqrt`, `exp`, `ln`, `log2`, `log10`, `rad`, `deg`                |
| Тригонометрия | `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh`   |
| Дата и время  | `day_of_week`, `day_of_month`, `days_in_month`, `day_of_year`, `minute`, `hour`, `month`, `year` |
| Тип           | `scalar`, `vector`                                                                               |
| Гистограмма   | `histogram_quantile`                                                                             |
| Другое        | `time`, `pi`                                                                                     |

**Примечание**: `histogram_quantile` использует линейную интерполяцию для классических бакетов гистограммы (определяемых меткой `le`). Нативные гистограммы пока не поддерживаются, а аргумент `phi` (уровень квантиля) в настоящее время должен быть константным скаляром — выражения, которые меняются на каждом шаге, например `histogram_quantile(time() / 1000, ...)`, отклоняются с ошибкой `NOT_IMPLEMENTED`.

<div id="operators">
  ### Операторы
</div>

Все арифметические (`+`, `-`, `*`, `/`, `%`, `^`), сравнительные (`==`, `!=`, `<`, `>`, `<=`, `>=` с необязательным модификатором `bool`) и логические (`and`, `or`, `unless`) бинарные операторы, с модификаторами `on()`/`ignoring()` и `group_left()`/`group_right()`.

Унарные операторы `+` и `-`.

<div id="aggregation-operators">
  ### Операторы агрегации
</div>

`sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `group`, `quantile`, `topk`, `bottomk`, `limitk` — с необязательными модификаторами `by()` и `without()`.

Пока не поддерживается: `count_values`.

<div id="example">
  ## Пример
</div>

```sql
SELECT * FROM prometheusQueryRange(mytable, 'rate(http_requests{job="prometheus"}[10m])[1h:10m]', now() - INTERVAL 10 MINUTES, now(), INTERVAL 1 MINUTE)
```