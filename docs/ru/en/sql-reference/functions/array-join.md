---
description: 'Документация по функции arrayJoin'
sidebar_label: 'arrayJoin'
slug: /sql-reference/functions/array-join
title: 'Функция arrayJoin'
doc_type: 'reference'
---

Это очень необычная функция.

Обычные функции не изменяют набор строк, а лишь меняют значения в каждой строке (map).
Агрегатные функции сворачивают набор строк (fold или reduce).
Функция `arrayJoin` берет каждую строку и порождает набор строк (unfold).

Эта функция принимает массив в качестве аргумента и разворачивает исходную строку в несколько строк — по числу элементов в массиве.
Все значения в столбцах просто копируются, кроме значений в столбце, к которому применяется эта функция: они заменяются соответствующим значением из массива.

:::note
Если массив пуст, `arrayJoin` не создает ни одной строки.
Чтобы вернуть одну строку, содержащую значение по умолчанию для типа массива, можно обернуть его в [emptyArrayToSingle](./array-functions.md#emptyArrayToSingle), например: `arrayJoin(emptyArrayToSingle(...))`.
:::

Например:

```sql title="Query"
SELECT arrayJoin([1, 2, 3] AS src) AS dst, 'Hello', src
```

```text title="Response"
┌─dst─┬─\'Hello\'─┬─src─────┐
│   1 │ Hello     │ [1,2,3] │
│   2 │ Hello     │ [1,2,3] │
│   3 │ Hello     │ [1,2,3] │
└─────┴───────────┴─────────┘
```

Функция `arrayJoin` влияет на все части запроса, включая предложение `WHERE`. Обратите внимание, что результат запроса ниже — `2`, хотя подзапрос вернул 1 строку.

```sql title="Query"
SELECT sum(1) AS impressions
FROM
(
    SELECT ['Istanbul', 'Berlin', 'Babruysk'] AS cities
)
WHERE arrayJoin(cities) IN ['Istanbul', 'Berlin'];
```

```text title="Response"
┌─impressions─┐
│           2 │
└─────────────┘
```

Запрос может использовать несколько функций `arrayJoin`. В этом случае преобразование выполняется несколько раз, а количество строк увеличивается.
Например:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    arrayJoin(cities) AS city,
    arrayJoin(browsers) AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

```text title="Response"
┌─impressions─┬─city─────┬─browser─┐
│           2 │ Istanbul │ Chrome  │
│           1 │ Istanbul │ Firefox │
│           2 │ Berlin   │ Chrome  │
│           1 │ Berlin   │ Firefox │
│           2 │ Babruysk │ Chrome  │
│           1 │ Babruysk │ Firefox │
└─────────────┴──────────┴─────────┘
```

<div id="important-note">
  ### Рекомендуемая практика
</div>

Использование нескольких `arrayJoin` с одним и тем же выражением может не дать ожидаемого результата из-за устранения общих подвыражений.
В таких случаях попробуйте изменить повторяющиеся выражения с массивами, добавив дополнительные операции, которые не влияют на результат JOIN. Например, `arrayJoin(arraySort(arr))`, `arrayJoin(arrayConcat(arr, []))`

Пример:

```sql title="Query"
SELECT
    arrayJoin(dice) AS first_throw,
    /* arrayJoin(dice) as second_throw */ -- is technically correct, but will annihilate result set
    arrayJoin(arrayConcat(dice, [])) AS second_throw -- intentionally changed expression to force re-evaluation
FROM (
    SELECT [1, 2, 3, 4, 5, 6] AS dice
);
```

Обратите внимание на синтаксис [`ARRAY JOIN`](../statements/select/array-join.md) в запросе SELECT, который открывает более широкие возможности.
`ARRAY JOIN` позволяет одновременно преобразовывать несколько массивов с одинаковым числом элементов.

Пример:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    city,
    browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
ARRAY JOIN
    cities AS city,
    browsers AS browser
GROUP BY
    2,
    3
```

```text title="Response"
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Babruysk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

Или можно использовать [`Tuple`](../data-types/tuple.md)

Пример:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    (arrayJoin(arrayZip(cities, browsers)) AS t).1 AS city,
    t.2 AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

```text title="Row"
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Babruysk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

Название `arrayJoin` в ClickHouse связано с его концептуальным сходством с операцией JOIN, но применительно к массивам внутри одной строки. Если традиционные JOIN объединяют строки из разных таблиц, то `arrayJoin` «соединяет» каждый элемент массива в строке, создавая несколько строк — по одной на каждый элемент массива — и при этом дублируя значения остальных столбцов. ClickHouse также поддерживает синтаксис оператора [`ARRAY JOIN`](/ru/sql-reference/statements/select/array-join), который делает эту связь с традиционными операциями JOIN ещё более явной за счёт использования привычной SQL-терминологии JOIN. Этот процесс также называют «разворачиванием» массива, однако термин «join» используется и в названии функции, и в операторе, поскольку по сути это похоже на соединение таблицы с элементами массива, то есть на расширение набора данных способом, аналогичным операции JOIN.