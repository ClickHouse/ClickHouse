---
toc_title: WITH
---

# Секция WITH {#with-clause}

Clickhouse поддерживает [Общие табличные выражения](https://ru.wikipedia.org/wiki/Иерархические_и_рекурсивные_запросы_в_SQL), то есть позволяет использовать результаты выражений из секции `WITH` в остальной части `SELECT` запроса. Именованные подзапросы могут быть включены в текущий и дочерний контекст запроса в тех местах, где разрешены табличные объекты. Рекурсия предотвращается путем скрытия общего табличного выражения текущего уровня из выражения `WITH`.

## Синтаксис

``` sql
WITH <expression> AS <identifier>
```
или
``` sql
WITH <identifier> AS <subquery expression>
```

## Примеры

**Пример 1:** Использование константного выражения как «переменной»

``` sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound;
```

**Пример 2:** Выкидывание выражения sum(bytes) из списка колонок в SELECT

``` sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s;
```

**Пример 3:** Использование результатов скалярного подзапроса

``` sql
/* запрос покажет TOP 10 самых больших таблиц */
WITH
    (
        SELECT sum(bytes)
        FROM system.parts
        WHERE active
    ) AS total_disk_usage
SELECT
    (sum(bytes) / total_disk_usage) * 100 AS table_disk_usage,
    table
FROM system.parts
GROUP BY table
ORDER BY table_disk_usage DESC
LIMIT 10;
```

**Пример 4:** Переиспользование выражения

``` sql
WITH test1 AS (SELECT i + 1, j + 1 FROM test1)
SELECT * FROM test1;
```

