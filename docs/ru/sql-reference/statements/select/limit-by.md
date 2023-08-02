---
sidebar_label: LIMIT BY
---

# Секция LIMIT BY {#limit-by-clause}

Запрос с секцией `LIMIT n BY expressions` выбирает первые `n` строк для каждого отличного значения `expressions`. Ключ `LIMIT BY` может содержать любое количество [выражений](../../syntax.md#syntax-expressions).

ClickHouse поддерживает следующий синтаксис:

-   `LIMIT [offset_value, ]n BY expressions`
-   `LIMIT n OFFSET offset_value BY expressions`

Во время обработки запроса, ClickHouse выбирает данные, упорядоченные по ключу сортировки. Ключ сортировки задаётся явно в секции [ORDER BY](order-by.md#select-order-by) или неявно в свойствах движка таблицы (порядок строк гарантирован только при использовании [ORDER BY](order-by.md#select-order-by), в ином случае блоки строк не будут упорядочены из-за многопоточной обработки). Затем ClickHouse применяет `LIMIT n BY expressions` и возвращает первые `n` для каждой отличной комбинации `expressions`. Если указан `OFFSET`, то для каждого блока данных, который принадлежит отдельной комбинации `expressions`, ClickHouse отступает `offset_value` строк от начала блока и возвращает не более `n`. Если `offset_value` больше, чем количество строк в блоке данных, ClickHouse не возвращает ни одной строки.

`LIMIT BY` не связана с секцией `LIMIT`. Их можно использовать в одном запросе.

Если вы хотите использовать в секции `LIMIT BY` номера столбцов вместо названий, включите настройку [enable_positional_arguments](../../../operations/settings/settings.md#enable-positional-arguments).	

## Примеры

Образец таблицы:

``` sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by values(1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Запросы:

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

Запрос `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` возвращает такой же результат.

Следующий запрос выбирает топ 5 рефереров для каждой пары `domain, device_type`, но не более 100 строк (`LIMIT n BY + LIMIT`).

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100
```

Запрос выберет топ 5 рефереров для каждой пары `domain, device_type`, но не более 100 строк (`LIMIT n BY + LIMIT`).

`LIMIT n BY` работает с [NULL](../../syntax.md#null-literal) как если бы это было конкретное значение. Т.е. в результате запроса пользователь получит все комбинации полей, указанных в `BY`.
