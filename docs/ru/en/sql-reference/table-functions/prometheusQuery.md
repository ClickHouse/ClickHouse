---
description: 'Выполняет запрос Prometheus, используя данные из таблицы TimeSeries.'
sidebar_label: 'prometheusQuery'
sidebar_position: 145
slug: /sql-reference/table-functions/prometheusQuery
title: 'prometheusQuery'
doc_type: 'reference'
---

Выполняет запрос Prometheus, используя данные из таблицы TimeSeries.

<div id="syntax">
  ## Синтаксис
</div>

```sql
prometheusQuery('db_name', 'time_series_table', 'promql_query', evaluation_time)
prometheusQuery(db_name.time_series_table, 'promql_query', evaluation_time)
prometheusQuery('time_series_table', 'promql_query', evaluation_time)
```

<div id="arguments">
  ## Аргументы
</div>

* `db_name` - Имя базы данных, в которой находится таблица TimeSeries.
* `time_series_table` - Имя таблицы TimeSeries.
* `promql_query` - Запрос, написанный на [PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/).
* `evaluation_time - Временная метка оценки. Чтобы вычислить запрос для текущего момента времени, используйте `now()`в качестве`evaluation&#95;time&#96;.

<div id="returned_value">
  ## Возвращаемое значение
</div>

Функция может возвращать разные столбцы в зависимости от типа результата запроса, переданного в параметре `promql_query`:

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

Мгновенные селекторы, диапазонные селекторы, условия по меткам (`=`, `!=`, `=~`, `!~`), модификаторы `offset`, модификаторы временной метки `@` и подзапросы.

<div id="functions">
  ### Функции
</div>

| Категория          | Функции                                                                                          |
| ------------------ | ------------------------------------------------------------------------------------------------ |
| Диапазон           | `rate`, `irate`, `delta`, `idelta`, `last_over_time`                                             |
| Математические     | `abs`, `sgn`, `floor`, `ceil`, `sqrt`, `exp`, `ln`, `log2`, `log10`, `rad`, `deg`                |
| Тригонометрические | `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh`   |
| Дата и время       | `day_of_week`, `day_of_month`, `days_in_month`, `day_of_year`, `minute`, `hour`, `month`, `year` |
| Тип                | `scalar`, `vector`                                                                               |
| Гистограмма        | `histogram_quantile`                                                                             |
| Другое             | `time`, `pi`                                                                                     |

**Примечание**: `histogram_quantile` использует линейную интерполяцию для классических бакетов гистограммы (определяемых меткой `le`). Нативные гистограммы пока не поддерживаются, а аргумент `phi` (уровень квантиля) сейчас должен быть константным `scalar` — expressions, меняющиеся на каждом шаге, такие как `histogram_quantile(time() / 1000, ...)`, отклоняются с ошибкой `NOT_IMPLEMENTED`.

<div id="operators">
  ### Операторы
</div>

Все арифметические (`+`, `-`, `*`, `/`, `%`, `^`), операторы сравнения (`==`, `!=`, `<`, `>`, `<=`, `>=` с необязательным `bool`) и логические (`and`, `or`, `unless`) бинарные операторы, с модификаторами `on()`/`ignoring()` и `group_left()`/`group_right()`.

Унарные операторы `+` и `-`.

<div id="aggregation-operators">
  ### Операторы агрегации
</div>

`sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `group`, `quantile`, `topk`, `bottomk`, `limitk` — с дополнительными модификаторами `by()` или `without()`.

Пока не поддерживается: `count_values`.

<div id="example">
  ## Пример
</div>

```sql
SELECT * FROM prometheusQuery(mytable, 'rate(http_requests{job="prometheus"}[10m])[1h:10m]', now())
```