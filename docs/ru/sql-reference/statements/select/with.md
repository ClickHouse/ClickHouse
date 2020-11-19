# Секция WITH {#sektsiia-with}

Данная секция представляет собой [Common Table Expressions](https://ru.wikipedia.org/wiki/Иерархические_и_рекурсивные_запросы_в_SQL), то есть позволяет использовать результаты выражений из секции `WITH` в остальной части `SELECT` запроса.  
 
### Ограничения

1. Рекурсивные запросы не поддерживаются
2. Если в качестве выражения используется подзапрос, то результат должен содержать ровно одну строку
3. Результаты выражений нельзя переиспользовать во вложенных запросах
В дальнейшем, результаты выражений можно использовать в секции SELECT.

### Примеры

**Пример 1:** Использование константного выражения как «переменной»

``` sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound
```

**Пример 2:** Выкидывание выражения sum(bytes) из списка колонок в SELECT

``` sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s
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
LIMIT 10
```

**Пример 4:** Переиспользование выражения

В настоящий момент, переиспользование выражения из секции WITH внутри подзапроса возможно только через дублирование.

``` sql
WITH ['hello'] AS hello
SELECT
    hello,
    *
FROM
(
    WITH ['hello'] AS hello
    SELECT hello
)
```

``` text
┌─hello─────┬─hello─────┐
│ ['hello'] │ ['hello'] │
└───────────┴───────────┘
```
