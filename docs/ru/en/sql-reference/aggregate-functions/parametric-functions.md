---
description: 'Документация по параметрическим агрегатным функциям'
sidebar_label: 'Параметрические'
sidebar_position: 38
slug: /sql-reference/aggregate-functions/parametric-functions
title: 'Параметрические агрегатные функции'
doc_type: 'reference'
---

Некоторые агрегатные функции могут принимать не только столбцы-аргументы (используемые для сжатия), но и набор параметров — констант, используемых для инициализации. В синтаксисе вместо одной пары скобок используются две. Первая — для параметров, вторая — для аргументов.

<div id="histogram">
  ## histogram
</div>

Вычисляет адаптивную гистограмму. Точность результата не гарантируется.

```sql
histogram(number_of_bins)(values)
```

Функция использует [A Streaming Parallel Decision Tree Algorithm](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf). Границы интервалов гистограммы корректируются по мере поступления новых данных. В общем случае ширина интервалов неодинакова.

**Аргументы**

`values` — [выражение](/ru/sql-reference/syntax#expressions), возвращающее входные значения.

**Параметры**

`number_of_bins` — Верхняя граница числа интервалов в гистограмме. Функция автоматически вычисляет количество интервалов. Она пытается достичь указанного числа интервалов, но если это не удается, использует меньшее число интервалов.

**Возвращаемые значения**

* [Array](../../sql-reference/data-types/array.md) из [Tuple](../../sql-reference/data-types/tuple.md) следующего формата:

  ```
  [(lower_1, upper_1, height_1), ... (lower_N, upper_N, height_N)]
  ```

  * `lower` — Нижняя граница интервала.
  * `upper` — Верхняя граница интервала.
  * `height` — Вычисленная высота интервала.

**Пример**

```sql
SELECT histogram(5)(number + 1)
FROM (
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

```text
┌─histogram(5)(plus(number, 1))───────────────────────────────────────────┐
│ [(1,4.5,4),(4.5,8.5,4),(8.5,12.75,4.125),(12.75,17,4.625),(17,20,3.25)] │
└─────────────────────────────────────────────────────────────────────────┘
```

Например, гистограмму можно построить с помощью функции [bar](/ru/sql-reference/functions/other-functions#bar):

```sql
WITH histogram(5)(rand() % 100) AS hist
SELECT
    arrayJoin(hist).3 AS height,
    bar(height, 0, 6, 5) AS bar
FROM
(
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

```text
┌─height─┬─bar───┐
│  2.125 │ █▋    │
│   3.25 │ ██▌   │
│  5.625 │ ████▏ │
│  5.625 │ ████▏ │
│  3.375 │ ██▌   │
└────────┴───────┘
```

В этом случае следует помнить, что границы интервалов гистограммы вам неизвестны.

<div id="sequencematch">
  ## sequenceMatch
</div>

Проверяет, содержит ли последовательность цепочку событий, соответствующую шаблону.

**Синтаксис**

```sql
sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
```

:::note
События, происходящие в одну и ту же секунду, могут располагаться в последовательности в неопределённом порядке, что может повлиять на результат.
:::

**Аргументы**

* `timestamp` — Столбец, содержащий данные о временной метке. Обычно используются типы данных `Date` и `дата и время`. Также можно использовать любой из поддерживаемых типов данных [UInt](../../sql-reference/data-types/int-uint.md).

* `cond1`, `cond2` — Условия, описывающие цепочку событий. Тип данных: `UInt8`. Можно передать до 32 аргументов-условий. Функция учитывает только события, описанные в этих условиях. Если последовательность содержит данные, не описанные ни в одном условии, функция их пропускает.

**Параметры**

* `pattern` — Строка шаблона. См. [Синтаксис шаблона](#pattern-syntax).

**Возвращаемые значения**

* 1, если шаблон совпал.
* 0, если шаблон не совпал.

Тип: `UInt8`.

<div id="pattern-syntax">
  #### Синтаксис шаблона
</div>

* `(?N)` — Соответствует аргументу условия в позиции `N`. Условия нумеруются в диапазоне `[1, 32]`. Например, `(?1)` соответствует аргументу, переданному в параметр `cond1`.

* `.*` — Соответствует любому количеству событий. Для сопоставления с этим элементом шаблона аргументы условий не требуются.

* `(?t operator value)` — Задаёт время в секундах, которое должно разделять два события. Например, шаблон `(?1)(?t>1800)(?2)` соответствует событиям, которые происходят с интервалом более 1800 секунд. Между этими событиями может находиться произвольное количество любых событий. Можно использовать операторы `>=`, `>`, `<`, `<=`, `==`.

**Примеры**

Рассмотрим данные в таблице `t`:

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
└──────┴────────┘
```

Выполните запрос:

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                     1 │
└───────────────────────────────────────────────────────────────────────┘
```

Функция нашла цепочку событий, в которой число 2 идёт после числа 1. Она пропустила число 3 между ними, потому что оно не описано как событие. Если мы хотим учитывать это число при поиске цепочки событий, приведённой в примере, следует задать для него условие.

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 3) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 3))─┐
│                                                                                        0 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

В этом случае функция не смогла найти цепочку событий, соответствующую шаблону, потому что событие с номером 3 произошло между 1 и 2. Если бы в этом же случае мы проверили условие для числа 4, последовательность соответствовала бы шаблону.

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 4) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**См. также**

* [sequenceCount](#sequencecount)

<div id="sequencecount">
  ## sequenceCount
</div>

Подсчитывает количество цепочек событий, соответствующих шаблону. Функция ищет неперекрывающиеся цепочки событий. Поиск следующей цепочки начинается после сопоставления текущей.

:::note
События, происходящие в одну и ту же секунду, могут располагаться в последовательности в неопределённом порядке, что может повлиять на результат.
:::

**Синтаксис**

```sql
sequenceCount(pattern)(timestamp, cond1, cond2, ...)
```

**Аргументы**

* `timestamp` — Столбец, содержащий данные о временной метке. Обычно используются типы данных `Date` и `дата и время`. Также можно использовать любые поддерживаемые типы данных [UInt](../../sql-reference/data-types/int-uint.md).

* `cond1`, `cond2` — Условия, описывающие цепочку событий. Тип данных: `UInt8`. Можно передать до 32 аргументов-условий. Функция учитывает только события, описанные в этих условиях. Если в последовательности есть данные, не описанные ни одним условием, функция их пропускает.

**Параметры**

* `pattern` — Строка шаблона. См. [Синтаксис шаблона](#pattern-syntax).

**Возвращаемые значения**

* Количество непересекающихся цепочек событий, соответствующих шаблону.

Тип: `UInt64`.

**Пример**

Рассмотрим данные в таблице `t`:

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

Подсчитайте, сколько раз число 2 встречается после числа 1, при этом между ними может находиться любое количество других чисел:

```sql
SELECT sequenceCount('(?1).*(?2)')(time, number = 1, number = 2) FROM t
```

```text
┌─sequenceCount('(?1).*(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                       2 │
└─────────────────────────────────────────────────────────────────────────┘
```

<div id="sequencematchevents">
  ## sequenceMatchEvents
</div>

Возвращает временные метки событий из самых длинных цепочек событий, соответствующих шаблону.

:::note
События, происходящие в одну и ту же секунду, могут располагаться в последовательности в неопределённом порядке, что влияет на результат.
:::

**Синтаксис**

```sql
sequenceMatchEvents(pattern)(timestamp, cond1, cond2, ...)
```

**Аргументы**

* `timestamp` — Столбец, содержащий данные о временной метке. Обычно используются типы данных `Date` и `дата и время`. Также можно использовать любой из поддерживаемых типов данных [UInt](../../sql-reference/data-types/int-uint.md).

* `cond1`, `cond2` — Условия, описывающие цепочку событий. Тип данных: `UInt8`. Можно передать до 32 аргументов-условий. Функция учитывает только события, описанные этими условиями. Если последовательность содержит данные, не описанные ни одним условием, функция пропускает их.

**Параметры**

* `pattern` — Строка шаблона. См. [Синтаксис шаблона](#pattern-syntax).

**Возвращаемые значения**

* Массив временных меток для совпавших аргументов-условий (?N) из цепочки событий. Позиция в массиве соответствует позиции аргумента условия в шаблоне.

Тип: Array.

**Пример**

Рассмотрим данные в таблице `t`:

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

Возвращает временные метки событий для самой длинной цепочки

```sql
SELECT sequenceMatchEvents('(?1).*(?2).*(?1)(?3)')(time, number = 1, number = 2, number = 4) FROM t
```

```text
┌─sequenceMatchEvents('(?1).*(?2).*(?1)(?3)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│ [1,3,4]                                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**См. также**

* [sequenceMatch](#sequencematch)

<div id="windowfunnel">
  ## windowFunnel
</div>

Ищет цепочки событий в скользящем временном окне и вычисляет максимальное число событий, произошедших в такой цепочке.

Функция работает по следующему алгоритму:

* Функция ищет данные, удовлетворяющие первому условию в цепочке, и устанавливает счётчик событий в 1. С этого момента начинается скользящее окно.

* Если события из цепочки происходят последовательно в пределах окна, счётчик увеличивается. Если последовательность событий нарушается, счётчик не увеличивается.

* Если данные содержат несколько цепочек событий с разной степенью завершённости, функция возвращает только длину самой длинной цепочки.

**Синтаксис**

```sql
windowFunnel(window, [mode, [mode, ... ]])(timestamp, cond1, cond2, ..., condN)
```

**Аргументы**

* `timestamp` — Имя столбца, содержащего временную метку. Поддерживаемые типы данных: [Date](../../sql-reference/data-types/date.md), [дата и время](/ru/sql-reference/data-types/datetime) и другие беззнаковые целочисленные типы (обратите внимание, что хотя `timestamp` поддерживает тип `UInt64`, его значение не может превышать максимальное значение Int64, то есть 2^63 - 1).
* `cond` — Условия или данные, описывающие цепочку событий. [UInt8](../../sql-reference/data-types/int-uint.md).

**Параметры**

* `window` — Длина скользящего окна, то есть временной интервал между первым и последним условием. Единица измерения `window` зависит от самого `timestamp` и может различаться. Определяется выражением `timestamp of cond1 <= timestamp of cond2 <= ... <= timestamp of condN <= timestamp of cond1 + window`.
* `mode` — Это необязательный аргумент. Можно задать один или несколько режимов.
  * `'strict_deduplication'` — Если одно и то же условие выполняется в последовательности событий, такое повторяющееся событие прерывает дальнейшую обработку. Note: может работать неожиданным образом, если для одного и того же события выполняются несколько условий.
  * `'strict_order'` — Не допускает появления других событий. Например, в случае `A->B->D->C` поиск `A->B->C` останавливается на `D`, и максимальный уровень события равен 2.
  * `'strict_increase'` — Применяет условия только к событиям со строго возрастающими временными метками.
  * `'strict_once'` — Учитывает каждое событие в цепочке только один раз, даже если оно удовлетворяет условию несколько раз.
  * `'allow_reentry'` — Игнорирует события, нарушающие строгий порядок. Например, в случае A-&gt;A-&gt;B-&gt;C находит A-&gt;B-&gt;C, игнорируя лишнее A, и максимальный уровень события равен 3.

**Возвращаемое значение**

Максимальное количество последовательных сработавших условий из цепочки в пределах скользящего временного окна.
Анализируются все цепочки в выборке.

Тип: `Integer`.

**Пример**

Определить, достаточно ли заданного периода времени, чтобы пользователь выбрал телефон и дважды купил его в интернет-магазине.

Зададим следующую цепочку событий:

1. Пользователь вошел в свой аккаунт в магазине (`eventID = 1003`).
2. Пользователь ищет телефон (`eventID = 1007, product = 'phone'`).
3. Пользователь оформил заказ (`eventID = 1009`).
4. Пользователь повторно оформил заказ (`eventID = 1010`).

Входная таблица:

```text
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-28 │       1 │ 2019-01-29 10:00:00 │    1003 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-31 │       1 │ 2019-01-31 09:00:00 │    1007 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-30 │       1 │ 2019-01-30 08:00:00 │    1009 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-02-01 │       1 │ 2019-02-01 08:00:00 │    1010 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
```

Узнайте, как далеко пользователь `user_id` смог пройти по цепочке в период с января по февраль 2019 года.

```sql title="Query"
SELECT
    level,
    count() AS c
FROM
(
    SELECT
        user_id,
        windowFunnel(6048000000000000)(timestamp, eventID = 1003, eventID = 1009, eventID = 1007, eventID = 1010) AS level
    FROM trend
    WHERE (event_date >= '2019-01-01') AND (event_date <= '2019-02-02')
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC;
```

```text title="Response"
┌─level─┬─c─┐
│     4 │ 1 │
└───────┴───┘
```

**Пример с режимом allow&#95;reentry**

В этом примере показано, как режим `allow_reentry` работает с шаблонами повторного входа пользователей:

```sql
-- Sample data: user visits checkout -> product detail -> checkout again -> payment
-- Without allow_reentry: stops at level 2 (product detail page)
-- With allow_reentry: reaches level 4 (payment completion)

SELECT
    level,
    count() AS users
FROM
(
    SELECT
        user_id,
        windowFunnel(3600, 'strict_order', 'allow_reentry')(
            timestamp,
            action = 'begin_checkout',      -- Step 1: Begin checkout
            action = 'view_product_detail', -- Step 2: View product detail  
            action = 'begin_checkout',      -- Step 3: Begin checkout again (reentry)
            action = 'complete_payment'     -- Step 4: Complete payment
        ) AS level
    FROM user_events
    WHERE event_date = today()
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC;
```

<div id="retention">
  ## retention
</div>

Функция принимает от 1 до 32 аргументов типа `UInt8` — набор условий, указывающих, было ли для события выполнено определённое условие.
В качестве аргумента можно указать любое условие (как в [WHERE](/ru/sql-reference/statements/select/where)).

Условия, кроме первого, применяются попарно: результат для второго будет true, если true первое и второе условие, для третьего — если true первое и третье, и т. д.

**Синтаксис**

```sql
retention(cond1, cond2, ..., cond32);
```

**Аргументы**

* `cond` — выражение, возвращающее значение `UInt8` (1 или 0).

**Возвращаемое значение**

Массив значений 1 или 0.

* 1 — условие для события выполнено.
* 0 — условие для события не выполнено.

Тип: `UInt8`.

**Пример**

Рассмотрим пример вычисления функции `retention` для определения трафика сайта.

**1.** Создайте таблицу для иллюстрации примера.

```sql title="Query"
CREATE TABLE retention_test(date Date, uid Int32) ENGINE = Memory;

INSERT INTO retention_test SELECT '2020-01-01', number FROM numbers(5);
INSERT INTO retention_test SELECT '2020-01-02', number FROM numbers(10);
INSERT INTO retention_test SELECT '2020-01-03', number FROM numbers(15);
```

Исходная таблица:

```sql title="Query"
SELECT * FROM retention_test
```

```text title="Response"
┌───────date─┬─uid─┐
│ 2020-01-01 │   0 │
│ 2020-01-01 │   1 │
│ 2020-01-01 │   2 │
│ 2020-01-01 │   3 │
│ 2020-01-01 │   4 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-02 │   0 │
│ 2020-01-02 │   1 │
│ 2020-01-02 │   2 │
│ 2020-01-02 │   3 │
│ 2020-01-02 │   4 │
│ 2020-01-02 │   5 │
│ 2020-01-02 │   6 │
│ 2020-01-02 │   7 │
│ 2020-01-02 │   8 │
│ 2020-01-02 │   9 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-03 │   0 │
│ 2020-01-03 │   1 │
│ 2020-01-03 │   2 │
│ 2020-01-03 │   3 │
│ 2020-01-03 │   4 │
│ 2020-01-03 │   5 │
│ 2020-01-03 │   6 │
│ 2020-01-03 │   7 │
│ 2020-01-03 │   8 │
│ 2020-01-03 │   9 │
│ 2020-01-03 │  10 │
│ 2020-01-03 │  11 │
│ 2020-01-03 │  12 │
│ 2020-01-03 │  13 │
│ 2020-01-03 │  14 │
└────────────┴─────┘
```

**2.** Сгруппируйте пользователей по уникальному идентификатору `uid` с помощью функции `retention`.

```sql title="Query"
SELECT
    uid,
    retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
FROM retention_test
WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
GROUP BY uid
ORDER BY uid ASC
```

```text title="Response"
┌─uid─┬─r───────┐
│   0 │ [1,1,1] │
│   1 │ [1,1,1] │
│   2 │ [1,1,1] │
│   3 │ [1,1,1] │
│   4 │ [1,1,1] │
│   5 │ [0,0,0] │
│   6 │ [0,0,0] │
│   7 │ [0,0,0] │
│   8 │ [0,0,0] │
│   9 │ [0,0,0] │
│  10 │ [0,0,0] │
│  11 │ [0,0,0] │
│  12 │ [0,0,0] │
│  13 │ [0,0,0] │
│  14 │ [0,0,0] │
└─────┴─────────┘
```

**3.** Вычислите общее количество посещений сайта в день.

```sql title="Query"
SELECT
    sum(r[1]) AS r1,
    sum(r[2]) AS r2,
    sum(r[3]) AS r3
FROM
(
    SELECT
        uid,
        retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
    FROM retention_test
    WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
    GROUP BY uid
)
```

```text title="Response"
┌─r1─┬─r2─┬─r3─┐
│  5 │  5 │  5 │
└────┴────┴────┘
```

Где:

* `r1` — количество уникальных посетителей, посетивших сайт 2020-01-01 (условие `cond1`).
* `r2` — количество уникальных посетителей, посетивших сайт за определённый период времени между 2020-01-01 и 2020-01-02 (условия `cond1` и `cond2`).
* `r3` — количество уникальных посетителей, посетивших сайт за определённый период времени 2020-01-01 и 2020-01-03 (условия `cond1` и `cond3`).

<div id="uniquptonx">
  ## uniqUpTo(N)(x)
</div>

Вычисляет количество различных значений аргумента до указанного предела `N`. Если количество различных значений аргумента больше `N`, эта функция возвращает `N` + 1, в противном случае вычисляет точное значение.

Рекомендуется использовать при небольших значениях `N`, до 10. Максимальное значение `N` — 100.

Для состояния агрегатной функции эта функция использует объём памяти, равный 1 + `N` * размер одного значения в байтах.
При работе со строками эта функция сохраняет некриптографический хеш размером 8 байт; для строк вычисление является приближённым.

Например, если у вас есть таблица, в которой регистрируется каждый поисковый запрос, выполненный пользователями на вашем веб-сайте. Каждая строка в таблице представляет собой один поисковый запрос, а столбцы содержат идентификатор пользователя, поисковый запрос и временную метку запроса. Вы можете использовать `uniqUpTo`, чтобы создать отчёт, показывающий только те ключевые слова, по которым было не менее 5 уникальных пользователей.

```sql
SELECT SearchPhrase
FROM SearchLog
GROUP BY SearchPhrase
HAVING uniqUpTo(4)(UserID) >= 5
```

`uniqUpTo(4)(UserID)` вычисляет количество уникальных значений `UserID` для каждого `SearchPhrase`, но считает только до 4 уникальных значений. Если для `SearchPhrase` имеется более 4 уникальных значений `UserID`, функция возвращает 5 (4 + 1). Затем условие `HAVING` отфильтровывает значения `SearchPhrase`, для которых количество уникальных значений `UserID` меньше 5. В результате вы получите список поисковых ключевых слов, которые использовались как минимум 5 уникальными пользователями.

<div id="summapfiltered">
  ## sumMapFiltered
</div>

Эта функция работает так же, как [sumMap](/ru/sql-reference/aggregate-functions/reference/summap), но дополнительно принимает в качестве параметра массив ключей для фильтрации. Это может быть особенно полезно при работе с ключами высокой мощности.

**Синтаксис**

`sumMapFiltered(keys_to_keep)(keys, values)`

**Параметры**

* `keys_to_keep`: [Array](../data-types/array.md) ключей, по которым выполняется фильтрация.
* `keys`: [Array](../data-types/array.md) ключей.
* `values`: [Array](../data-types/array.md) значений.

**Возвращаемое значение**

* Возвращает кортеж из двух массивов: ключи в отсортированном порядке и значения, просуммированные для соответствующих ключей.

**Пример**

```sql title="Query"
CREATE TABLE sum_map
(
    `date` Date,
    `timeslot` DateTime,
    `statusMap` Nested(status UInt16, requests UInt64)
)
ENGINE = Log

INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);
```

```sql title="Query"
SELECT sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests) FROM sum_map;
```

```response title="Response"
   ┌─sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests)─┐
1. │ ([1,4,8],[10,20,10])                                            │
   └─────────────────────────────────────────────────────────────────┘
```

<div id="summapfilteredwithoverflow">
  ## sumMapFilteredWithOverflow
</div>

Эта функция работает так же, как [sumMap](/ru/sql-reference/aggregate-functions/reference/summap), но дополнительно принимает в качестве параметра массив ключей, по которым нужно выполнять фильтрацию. Это особенно полезно при работе с высокой мощностью ключей. От функции [sumMapFiltered](#summapfiltered) она отличается тем, что выполняет суммирование с переполнением, то есть возвращает результат суммирования в том же типе данных, что и аргумент.

**Синтаксис**

`sumMapFilteredWithOverflow(keys_to_keep)(keys, values)`

**Параметры**

* `keys_to_keep`: [Array](../data-types/array.md) ключей, по которым выполняется фильтрация.
* `keys`: [Array](../data-types/array.md) ключей.
* `values`: [Array](../data-types/array.md) значений.

**Возвращаемое значение**

* Возвращает кортеж из двух массивов: ключи в отсортированном порядке и значения, суммированные для соответствующих ключей.

**Пример**

В этом примере мы создаем таблицу `sum_map`, вставляем в нее данные, а затем используем `sumMapFilteredWithOverflow`, `sumMapFiltered` и функцию `toTypeName`, чтобы сравнить результаты. Поскольку `requests` в созданной таблице имел тип `UInt8`, `sumMapFiltered` повысила тип суммированных значений до `UInt64`, чтобы избежать переполнения, тогда как `sumMapFilteredWithOverflow` сохранила тип `UInt8`, которого недостаточно для хранения результата, — то есть произошло переполнение.

```sql title="Query"
CREATE TABLE sum_map
(
    `date` Date,
    `timeslot` DateTime,
    `statusMap` Nested(status UInt8, requests UInt8)
)
ENGINE = Log

INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);
```

```sql title="Query"
SELECT sumMapFilteredWithOverflow([1, 4, 8])(statusMap.status, statusMap.requests) as summap_overflow, toTypeName(summap_overflow) FROM sum_map;
```

```sql title="Query"
SELECT sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests) as summap, toTypeName(summap) FROM sum_map;
```

```response title="Response"
   ┌─sum──────────────────┬─toTypeName(sum)───────────────────┐
1. │ ([1,4,8],[10,20,10]) │ Tuple(Array(UInt8), Array(UInt8)) │
   └──────────────────────┴───────────────────────────────────┘
```

```response title="Response"
   ┌─summap───────────────┬─toTypeName(summap)─────────────────┐
1. │ ([1,4,8],[10,20,10]) │ Tuple(Array(UInt8), Array(UInt64)) │
   └──────────────────────┴────────────────────────────────────┘
```

<div id="sequencenextnode">
  ## sequenceNextNode
</div>

Возвращает значение следующего события, соответствующего цепочке событий.

*Экспериментальная функция; чтобы включить её, задайте `SET allow_experimental_funnel_functions = 1`.*

**Синтаксис**

```sql
sequenceNextNode(direction, base)(timestamp, event_column, base_condition, event1, event2, event3, ...)
```

**Параметры**

* `direction` — Используется для выбора направления.
  * forward — Движение вперёд.
  * backward — Движение назад.

* `base` — Используется для задания базовой точки.
  * head — Установить базовую точку на первое событие.
  * tail — Установить базовую точку на последнее событие.
  * first&#95;match — Установить базовую точку на первое совпавшее `event1`.
  * last&#95;match — Установить базовую точку на последнее совпавшее `event1`.

**Аргументы**

* `timestamp` — Имя столбца, содержащего временную метку. Поддерживаемые типы данных: [Date](../../sql-reference/data-types/date.md), [дата и время](/ru/sql-reference/data-types/datetime) и другие беззнаковые целочисленные типы.
* `event_column` — Имя столбца, содержащего значение следующего события, которое нужно вернуть. Поддерживаемые типы данных: [String](../../sql-reference/data-types/string.md) и [Nullable(String)](../../sql-reference/data-types/nullable.md).
* `base_condition` — Условие, которому должна удовлетворять базовая точка.
* `event1`, `event2`, ... — Условия, описывающие цепочку событий. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемые значения**

* `event_column[next_index]` — Если цепочка событий совпала и существует следующее значение.
* `NULL` - Если цепочка событий не совпала или следующего значения не существует.

Тип: [Nullable(String)](../../sql-reference/data-types/nullable.md).

**Пример**

Можно использовать, когда события идут как A-&gt;B-&gt;C-&gt;D-&gt;E и нужно узнать, какое событие следует за B-&gt;C, то есть D.

Оператор запроса, который ищет событие, следующее за A-&gt;B:

```sql title="Query"
CREATE TABLE test_flow (
    dt DateTime,
    id int,
    page String)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(dt)
ORDER BY id;

INSERT INTO test_flow VALUES (1, 1, 'A') (2, 1, 'B') (3, 1, 'C') (4, 1, 'D') (5, 1, 'E');

SELECT id, sequenceNextNode('forward', 'head')(dt, page, page = 'A', page = 'A', page = 'B') as next_flow FROM test_flow GROUP BY id;
```

```text title="Response"
┌─id─┬─next_flow─┐
│  1 │ C         │
└────┴───────────┘
```

**Поведение `forward` и `head`**

```sql
ALTER TABLE test_flow DELETE WHERE 1 = 1 settings mutations_sync = 1;

INSERT INTO test_flow VALUES (1, 1, 'Home') (2, 1, 'Gift') (3, 1, 'Exit');
INSERT INTO test_flow VALUES (1, 2, 'Home') (2, 2, 'Home') (3, 2, 'Gift') (4, 2, 'Basket');
INSERT INTO test_flow VALUES (1, 3, 'Gift') (2, 3, 'Home') (3, 3, 'Gift') (4, 3, 'Basket');
```

```sql
SELECT id, sequenceNextNode('forward', 'head')(dt, page, page = 'Home', page = 'Home', page = 'Gift') FROM test_flow GROUP BY id;

                  dt   id   page
 1970-01-01 09:00:01    1   Home // Base point, Matched with Home
 1970-01-01 09:00:02    1   Gift // Matched with Gift
 1970-01-01 09:00:03    1   Exit // The result

 1970-01-01 09:00:01    2   Home // Base point, Matched with Home
 1970-01-01 09:00:02    2   Home // Unmatched with Gift
 1970-01-01 09:00:03    2   Gift
 1970-01-01 09:00:04    2   Basket

 1970-01-01 09:00:01    3   Gift // Base point, Unmatched with Home
 1970-01-01 09:00:02    3   Home
 1970-01-01 09:00:03    3   Gift
 1970-01-01 09:00:04    3   Basket
```

**Поведение параметров `backward` и `tail`**

```sql
SELECT id, sequenceNextNode('backward', 'tail')(dt, page, page = 'Basket', page = 'Basket', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift
1970-01-01 09:00:03    1   Exit // Base point, Unmatched with Basket

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home // The result
1970-01-01 09:00:03    2   Gift // Matched with Gift
1970-01-01 09:00:04    2   Basket // Base point, Matched with Basket

1970-01-01 09:00:01    3   Gift
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift // Base point, Matched with Gift
1970-01-01 09:00:04    3   Basket // Base point, Matched with Basket
```

**Поведение параметров `forward` и `first_match`**

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, page = 'Gift', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift // Base point
1970-01-01 09:00:03    1   Exit // The result

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home
1970-01-01 09:00:03    2   Gift // Base point
1970-01-01 09:00:04    2   Basket  The result

1970-01-01 09:00:01    3   Gift // Base point
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift
1970-01-01 09:00:04    3   Basket
```

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, page = 'Gift', page = 'Gift', page = 'Home') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift // Base point
1970-01-01 09:00:03    1   Exit // Unmatched with Home

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home
1970-01-01 09:00:03    2   Gift // Base point
1970-01-01 09:00:04    2   Basket // Unmatched with Home

1970-01-01 09:00:01    3   Gift // Base point
1970-01-01 09:00:02    3   Home // Matched with Home
1970-01-01 09:00:03    3   Gift // The result
1970-01-01 09:00:04    3   Basket
```

**Поведение параметров `backward` и `last_match`**

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, page = 'Gift', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home // The result
1970-01-01 09:00:02    1   Gift // Base point
1970-01-01 09:00:03    1   Exit

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home // The result
1970-01-01 09:00:03    2   Gift // Base point
1970-01-01 09:00:04    2   Basket

1970-01-01 09:00:01    3   Gift
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift // Base point
1970-01-01 09:00:04    3   Basket
```

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, page = 'Gift', page = 'Gift', page = 'Home') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home // Matched with Home, the result is null
1970-01-01 09:00:02    1   Gift // Base point
1970-01-01 09:00:03    1   Exit

1970-01-01 09:00:01    2   Home // The result
1970-01-01 09:00:02    2   Home // Matched with Home
1970-01-01 09:00:03    2   Gift // Base point
1970-01-01 09:00:04    2   Basket

1970-01-01 09:00:01    3   Gift // The result
1970-01-01 09:00:02    3   Home // Matched with Home
1970-01-01 09:00:03    3   Gift // Base point
1970-01-01 09:00:04    3   Basket
```

**Поведение `base_condition`**

```sql
CREATE TABLE test_flow_basecond
(
    `dt` DateTime,
    `id` int,
    `page` String,
    `ref` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(dt)
ORDER BY id;

INSERT INTO test_flow_basecond VALUES (1, 1, 'A', 'ref4') (2, 1, 'A', 'ref3') (3, 1, 'B', 'ref2') (4, 1, 'B', 'ref1');
```

```sql
SELECT id, sequenceNextNode('forward', 'head')(dt, page, ref = 'ref1', page = 'A') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4 // The head can not be base point because the ref column of the head unmatched with 'ref1'.
 1970-01-01 09:00:02    1   A      ref3
 1970-01-01 09:00:03    1   B      ref2
 1970-01-01 09:00:04    1   B      ref1
```

```sql
SELECT id, sequenceNextNode('backward', 'tail')(dt, page, ref = 'ref4', page = 'B') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4
 1970-01-01 09:00:02    1   A      ref3
 1970-01-01 09:00:03    1   B      ref2
 1970-01-01 09:00:04    1   B      ref1 // The tail can not be base point because the ref column of the tail unmatched with 'ref4'.
```

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, ref = 'ref3', page = 'A') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4 // This row can not be base point because the ref column unmatched with 'ref3'.
 1970-01-01 09:00:02    1   A      ref3 // Base point
 1970-01-01 09:00:03    1   B      ref2 // The result
 1970-01-01 09:00:04    1   B      ref1
```

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, ref = 'ref2', page = 'B') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4
 1970-01-01 09:00:02    1   A      ref3 // The result
 1970-01-01 09:00:03    1   B      ref2 // Base point
 1970-01-01 09:00:04    1   B      ref1 // This row can not be base point because the ref column unmatched with 'ref2'.
```