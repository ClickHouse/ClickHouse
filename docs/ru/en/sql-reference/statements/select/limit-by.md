---
description: 'Документация по предложению LIMIT BY'
sidebar_label: 'LIMIT BY'
slug: /sql-reference/statements/select/limit-by
title: 'Предложение LIMIT BY'
doc_type: 'reference'
---

Запрос с предложением `LIMIT n BY expressions` выбирает первые `n` строк для каждого уникального значения `expressions`. Ключ для `LIMIT BY` может содержать любое количество [выражений](/ru/sql-reference/syntax#expressions).

ClickHouse поддерживает следующие варианты синтаксиса:

* `LIMIT [offset_value, ]n BY expressions`
* `LIMIT n OFFSET offset_value BY expressions`

При обработке запроса ClickHouse выбирает данные, упорядоченные по ключу сортировки. Ключ сортировки задаётся явно с помощью предложения [ORDER BY](/ru/sql-reference/statements/select/order-by) или неявно как свойство движка таблицы (порядок строк гарантируется только при использовании [ORDER BY](/ru/sql-reference/statements/select/order-by), в противном случае из-за многопоточности блоки строк не будут упорядочены). Затем ClickHouse применяет `LIMIT n BY expressions` и возвращает первые `n` строк для каждой уникальной комбинации `expressions`. Если указан `OFFSET`, то для каждого блока данных, относящегося к уникальной комбинации `expressions`, ClickHouse пропускает `offset_value` строк с начала блока и возвращает не более `n` строк. Если `offset_value` больше количества строк в блоке данных, ClickHouse не возвращает из этого блока ни одной строки.

:::note
`LIMIT BY` не связан с [LIMIT](../../../sql-reference/statements/select/limit.md). Оба предложения можно использовать в одном запросе.
:::

Если вы хотите использовать номера столбцов вместо их имён в предложении `LIMIT BY`, включите настройку [enable&#95;positional&#95;arguments](/ru/operations/settings/settings#enable_positional_arguments).

<div id="examples">
  ## Примеры
</div>

Таблица-пример:

```sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Запросы:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

Запрос `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` возвращает тот же результат.

Следующий запрос возвращает 5 лучших источников переходов для каждой пары `domain, device_type`, при этом общее число строк не превышает 100 (`LIMIT n BY + LIMIT`).

```sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100;
```

`LIMIT BY` также работает с отрицательными ограничениями и смещениями. Аналогично [отрицательному LIMIT](/ru/sql-reference/statements/select/limit#negative-limits), с `LIMIT BY` можно использовать отрицательные значения, чтобы выбирать строки с *конца* каждой группы.

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

Возвращает последние 2 строки для каждого `id`. Для `id = 1` будут получены строки `11` и `12`; для `id = 2` возвращаются обе строки, поскольку в группе всего 2 строки.

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -1 OFFSET -1 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  2 │  20 │
└────┴─────┘
```

Возвращает предпоследнюю строку для каждого `id`: конечный `OFFSET -1` отбрасывает последнюю строку в каждой группе, а начальный `-1` затем оставляет последнюю строку из оставшихся.

`LIMIT` и `OFFSET` с разными знаками тоже можно комбинировать. Например, чтобы отбросить первую строку в каждой группе, а затем оставить последние 2 из оставшихся:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -2 OFFSET 1 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

Для `id = 1` первая строка (`10`) пропускается; возвращаются обе последние строки — `11` и `12`. Для `id = 2` первая строка (`20`) пропускается, и остаётся только `21`.

<div id="limit-by-all">
  ## LIMIT BY ALL
</div>

`LIMIT BY ALL` равносилен перечислению всех выражений из SELECT, которые не являются агрегатными функциями.

Например:

```sql
SELECT col1, col2, col3 FROM table LIMIT 2 BY ALL;
```

то же, что и

```sql
SELECT col1, col2, col3 FROM table LIMIT 2 BY col1, col2, col3;
```

В особом случае, если у функции среди аргументов есть и агрегатные функции, и другие поля, ключи `LIMIT BY` будут включать максимально возможное число неагрегатных полей, которые можно из неё извлечь.

Например:

```sql
SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t LIMIT 2 BY ALL;
```

то же, что и

```sql
SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t LIMIT 2 BY substring(a, 4, 2), substring(a, 1, 2);
```

<div id="examples">
  ## Примеры
</div>

Пример таблицы:

```sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Запросы:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

Запрос `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` возвращает тот же результат.

Использование `LIMIT BY ALL`:

```sql
SELECT id, val FROM limit_by ORDER BY id, val LIMIT 2 BY ALL;
```

Это эквивалентно:

```sql
SELECT id, val FROM limit_by ORDER BY id, val LIMIT 2 BY id, val;
```