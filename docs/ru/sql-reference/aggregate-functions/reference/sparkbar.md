---
slug: /ru/sql-reference/aggregate-functions/reference/sparkbar
sidebar_position: 311
sidebar_label: sparkbar
---

# sparkbar {#sparkbar}

Функция строит гистограмму частот по заданным значениям `x` и частоте повторения этих значений `y` на интервале `[min_x, max_x]`. Повторения для всех `x`, попавших в один бакет, усредняются, поэтому данные должны быть предварительно агрегированы. Отрицательные повторения игнорируются.

Если интервал для построения не указан, то в качестве нижней границы интервала будет взято минимальное значение `x`, а в качестве верхней границы — максимальное значение `x`.
Значения `x` вне указанного интервала игнорируются.


**Синтаксис**

``` sql
sparkbar(width[, min_x, max_x])(x, y)
```

**Параметры**

-   `width` — Количество столбцов гистограммы. Тип: [Integer](../../../sql-reference/data-types/int-uint.md).

-   `min_x` — Начало интервала. Необязательный параметр.
-   `max_x` — Конец интервала. Необязательный параметр.

**Аргументы**

-   `x` — Поле со значениями.
-   `y` — Поле с частотой повторения значений.


**Возвращаемые значения**

-   Гистограмма частот.

**Пример**

Запрос:

``` sql
CREATE TABLE spark_bar_data (`value` Int64, `event_date` Date) ENGINE = MergeTree ORDER BY event_date;

INSERT INTO spark_bar_data VALUES (1,'2020-01-01'), (3,'2020-01-02'), (4,'2020-01-02'), (-3,'2020-01-02'), (5,'2020-01-03'), (2,'2020-01-04'), (3,'2020-01-05'), (7,'2020-01-06'), (6,'2020-01-07'), (8,'2020-01-08'), (2,'2020-01-11');

SELECT sparkbar(9)(event_date,cnt) FROM (SELECT sum(value) as cnt, event_date FROM spark_bar_data GROUP BY event_date);

SELECT sparkbar(9, toDate('2020-01-01'), toDate('2020-01-10'))(event_date,cnt) FROM (SELECT sum(value) as cnt, event_date FROM spark_bar_data GROUP BY event_date);
```

Результат:

``` text
┌─sparkbar(9)(event_date, cnt)─┐
│ ▂▅▂▃▆█  ▂                    │
└──────────────────────────────┘

┌─sparkbar(9, toDate('2020-01-01'), toDate('2020-01-10'))(event_date, cnt)─┐
│ ▂▅▂▃▇▆█                                                                  │
└──────────────────────────────────────────────────────────────────────────┘
```
