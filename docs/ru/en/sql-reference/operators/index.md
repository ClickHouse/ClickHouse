---
description: 'Документация по операторам'
sidebar_label: 'Операторы'
sidebar_position: 38
slug: /sql-reference/operators/
title: 'Операторы'
doc_type: 'reference'
---

ClickHouse преобразует операторы в соответствующие функции на этапе разбора запроса в соответствии с их приоритетом, старшинством и ассоциативностью.

<div id="access-operators">
  ## Операторы доступа
</div>

`a[N]` — доступ к элементу массива. Функция `arrayElement(a, N)`.

`a.N` — доступ к элементу кортежа. Функция `tupleElement(a, N)`.

<div id="numeric-negation-operator">
  ## Оператор числового отрицания
</div>

`-a` — функция `negate (a)`.

Для отрицания кортежей: [tupleNegate](../../sql-reference/functions/tuple-functions.md#tupleNegate).

<div id="multiplication-and-division-operators">
  ## Операторы умножения и деления
</div>

`a * b` — функция `multiply (a, b)`.

Для умножения кортежа на число см. [tupleMultiplyByNumber](../../sql-reference/functions/tuple-functions.md#tupleMultiplyByNumber), для скалярного произведения — [dotProduct](/ru/sql-reference/functions/array-functions#arrayDotProduct).

`a / b` — функция `divide(a, b)`.

Для деления кортежа на число см. [tupleDivideByNumber](../../sql-reference/functions/tuple-functions.md#tupleDivideByNumber).

`a % b` — функция `modulo(a, b)`.

<div id="addition-and-subtraction-operators">
  ## Операторы сложения и вычитания
</div>

`a + b` — функция `plus(a, b)`.

Для сложения кортежей: [tuplePlus](../../sql-reference/functions/tuple-functions.md#tuplePlus).

`a - b` — функция `minus(a, b)`.

Для вычитания кортежей: [tupleMinus](../../sql-reference/functions/tuple-functions.md#tupleMinus).

<div id="comparison-operators">
  ## Операторы сравнения
</div>

<div id="equals-function">
  ### Функция equals
</div>

`a = b` – функция `equals(a, b)`.

`a == b` – функция `equals(a, b)`.

<div id="notequals-function">
  ### Функция notEquals
</div>

`a != b` — функция `notEquals(a, b)`.

`a <> b` — функция `notEquals(a, b)`.

<div id="lessorequals-function">
  ### Функция lessOrEquals
</div>

`a <= b` — функция `lessOrEquals(a, b)`.

<div id="greaterorequals-function">
  ### Функция greaterOrEquals
</div>

`a >= b` — функция `greaterOrEquals(a, b)`.

<div id="less-function">
  ### функция less
</div>

`a < b` — функция `less(a, b)`.

<div id="greater-function">
  ### функция greater
</div>

`a > b` — функция `greater(a, b)`.

<div id="like-function">
  ### Функция LIKE
</div>

`a LIKE b` — функция `like(a, b)`.

<div id="notlike-function">
  ### Функция notLike
</div>

`a NOT LIKE b` — функция `notLike(a, b)`.

<div id="ilike-function">
  ### Функция ilike
</div>

`a ILIKE b` — функция `ilike(a, b)`.

<div id="between-function">
  ### Функция BETWEEN
</div>

`a BETWEEN b AND c` — эквивалентно `a >= b AND a <= c`.

`a NOT BETWEEN b AND c` — эквивалентно `a < b OR a > c`.

<div id="is-not-distinct-from">
  ### оператор IS NOT DISTINCT FROM (`<=>`)
</div>

:::note
Начиная с версии 25.10 `<=>` можно использовать так же, как любой другой оператор.
До версии 25.10 его можно было использовать только в выражениях JOIN, например:

```sql
CREATE TABLE a (x String) ENGINE = Memory;
INSERT INTO a VALUES ('ClickHouse');

SELECT * FROM a AS a1 JOIN a AS a2 ON a1.x <=> a2.x;

┌─x──────────┬─a2.x───────┐
│ ClickHouse │ ClickHouse │
└────────────┴────────────┘
```

:::

Оператор `<=>` — это `NULL`-безопасный оператор равенства, эквивалентный `IS NOT DISTINCT FROM`.
Он работает как обычный оператор равенства (`=`), но считает значения `NULL` сопоставимыми.
Два значения `NULL` считаются равными, а при сравнении `NULL` с любым значением, отличным от `NULL`, возвращается 0 (`false`), а не `NULL`.

```sql
SELECT
  'ClickHouse' <=> NULL,
  NULL <=> NULL
```

```response
┌─isNotDistinc⋯use', NULL)─┬─isNotDistinc⋯NULL, NULL)─┐
│                        0 │                        1 │
└──────────────────────────┴──────────────────────────┘
```

<div id="operators-for-working-with-strings">
  ## Операторы для работы со строками
</div>

<div id="overlay">
  ### OVERLAY
</div>

* `OVERLAY(string PLACING replacement FROM offset)` — функция `overlay(string, replacement, offset)`.
* `OVERLAY(string PLACING replacement FROM offset FOR length)` — функция `overlay(string, replacement, offset, length)`.
* `OVERLAYUTF8(string PLACING replacement FROM offset)` — функция `overlayUTF8(string, replacement, offset)`.
* `OVERLAYUTF8(string PLACING replacement FROM offset FOR length)` — функция `overlayUTF8(string, replacement, offset, length)`.

<div id="operators-for-working-with-data-sets">
  ## Операторы для работы с наборами данных
</div>

См. [операторы IN](../../sql-reference/operators/in.md) и оператор [EXISTS](../../sql-reference/operators/exists.md).

<div id="in-function">
  ### функция in
</div>

`a IN ...` — функция `in(a, b)`.

<div id="notin-function">
  ### Функция notIn
</div>

`a NOT IN ...` — функция `notIn(a, b)`.

<div id="globalin-function">
  ### Функция globalIn
</div>

`a GLOBAL IN ...` — функция `globalIn(a, b)`.

<div id="globalnotin-function">
  ### Функция globalNotIn
</div>

`a GLOBAL NOT IN ...` — функция `globalNotIn(a, b)`.

<div id="in-subquery-function">
  ### функция in subquery
</div>

`a = ANY (subquery)` — функция `in(a, subquery)`.

<div id="notin-subquery-function">
  ### функция notIn с подзапросом
</div>

`a != ANY (subquery)` — то же, что `a NOT IN (SELECT singleValueOrNull(*) FROM subquery)`.

<div id="in-subquery-function-1">
  ### Функция in subquery
</div>

`a = ALL (subquery)` — то же самое, что и `a IN (SELECT singleValueOrNull(*) FROM subquery)`.

<div id="notin-subquery-function">
  ### функция notIn с подзапросом
</div>

`a != ALL (subquery)` — функция `notIn(a, subquery)`.

**Примеры**

Запрос с ALL:

```sql title="Query"
SELECT number AS a FROM numbers(10) WHERE a > ALL (SELECT number FROM numbers(3, 3));
```

```text title="Response"
┌─a─┐
│ 6 │
│ 7 │
│ 8 │
│ 9 │
└───┘
```

Запрос с ANY:

```sql title="Query"
SELECT number AS a FROM numbers(10) WHERE a > ANY (SELECT number FROM numbers(3, 3));
```

```text title="Response"
┌─a─┐
│ 4 │
│ 5 │
│ 6 │
│ 7 │
│ 8 │
│ 9 │
└───┘
```

<div id="some-all-on-arrays">
  ### `SOME` / `ALL` для массивов
</div>

Помимо формы с подзапросом, описанной выше, в правой части `SOME` / `ALL` может использоваться выражение, возвращающее массив (литерал массива, столбец типа массива или любое другое выражение, возвращающее массив). Это синтаксис квантификатора массива в стиле PostgreSQL. Он распознаётся на этапе разбора и переписывается в функции для работы с массивами, поэтому вручную ничего переписывать не нужно:

| Синтаксис                                                  | Переписывается в                   |
| ---------------------------------------------------------- | ---------------------------------- |
| `expr = SOME(arr)`                                         | `has(arr, expr)`                   |
| `expr <> ALL(arr)`                                         | `NOT has(arr, expr)`               |
| `expr OP SOME(arr)` (любой другой поддерживаемый оператор) | `arrayExists(x -> expr OP x, arr)` |
| `expr OP ALL(arr)` (любой другой поддерживаемый оператор)  | `arrayAll(x -> expr OP x, arr)`    |

`SOME` — это квантификатор существования (синоним `ANY` в SQL). Для `=` и `<>` используется специальное преобразование в `has` / `NOT has`, поскольку для них есть оптимизированная реализация; в общем случае используются функции высшего порядка `arrayExists` / `arrayAll`.

Форма массива распознаётся для операторов сравнения `=`, `==`, `!=`, `<>`, `<=>`, `<`, `<=`, `>`, `>=`, предикатов сравнения с ключевыми словами `IS DISTINCT FROM` и `IS NOT DISTINCT FROM`, а также строковых предикатов поиска `LIKE`, `ILIKE`, `NOT LIKE`, `NOT ILIKE` и `REGEXP`. Предикаты сравнения с ключевыми словами и строковые предикаты поиска распознаются только для формы массива, но не для формы с подзапросом (которая приводится к `IN`/`NOT IN`). Операторы, для которых квантификатор массива не имеет смысла, — например, сам `IN` — **не** переписываются и сохраняют своё обычное значение.

Строковые предикаты поиска работают, потому что `MatchImpl` (реализация, лежащая в основе `LIKE` / `ILIKE` / `REGEXP`) поддерживает константный haystack с неконстантной needle. Например, `'abc' LIKE SOME(['a%', 'b%'])` переписывается в `arrayExists(x -> 'abc' LIKE x, ['a%', 'b%'])`, а `'abc' NOT LIKE ALL(['x%', 'y%'])` — в `arrayAll(x -> 'abc' NOT LIKE x, ['x%', 'y%'])`. Это позволяет сопоставить одну строку с несколькими шаблонами; если нужно выполнить сопоставление за один общий проход, по-прежнему можно использовать функцию поиска по нескольким шаблонам, например `multiMatchAny` (регулярные выражения) или `multiSearchAny` (подстроки).

:::note `ANY` is not supported for the array form
Только `SOME` и `ALL` принимают массив в правой части. `ANY` исключён, потому что `any` также является агрегатной функцией, поэтому выражение вида `expr = any(x)` сохраняет смысл вызова функции. Для квантификатора массива используйте `SOME`.
:::

```sql title="Query"
SELECT
    3 = SOME([1, 2, 3, 4])         AS in_array,
    5 < SOME([1, 2, 6])            AS less_than_some,
    5 > ALL([1, 2, 3])             AS greater_than_all,
    'abc' LIKE SOME(['a%', 'z%'])  AS like_some;
```

```text title="Response"
┌─in_array─┬─less_than_some─┬─greater_than_all─┬─like_some─┐
│        1 │              1 │                1 │         1 │
└──────────┴────────────────┴──────────────────┴───────────┘
```

:::note Обработка `NULL` отличается от формы с подзапросом
Поскольку форма массива преобразуется парсером (где настройки запроса, такие как `transform_null_in`, недоступны, а столбец типа Array в каждой строке не может использовать null-safe путь `IN` анализатора), она использует двухзначную семантику `has` (для `=` / `<>`) и `arrayExists` / `arrayAll` (которые сводят неизвестный результат сравнения с `NULL` к `0`). Это может отличаться от формы с подзапросом, где обработка `NULL` реализуется через `IN` / `NOT IN` и зависит от `transform_null_in`:

```sql
SELECT NULL = SOME([NULL]);   -- has([NULL], NULL)                  -> 1
SELECT NULL <> ALL([NULL]);   -- NOT has([NULL], NULL)              -> 0
SELECT NULL < SOME([1]);      -- arrayExists(x -> NULL < x, [1])    -> 0
SELECT NULL > ALL([1]);       -- arrayAll(x -> NULL > x, [1])       -> 0
```

:::

<div id="operators-for-working-with-dates-and-times">
  ## Операторы для работы с датами и временем
</div>

<div id="extract">
  ### EXTRACT
</div>

```sql
EXTRACT(part FROM date);
```

Извлекает части из указанной даты. Например, можно получить месяц из даты или секунду из времени.

Параметр `part` указывает, какую часть даты нужно извлечь. Доступны следующие значения:

* `NANOSECOND` — наносекунда. Возможные значения: 0–999999999.
* `MICROSECOND` — микросекунда. Возможные значения: 0–999999.
* `MILLISECOND` — миллисекунда. Возможные значения: 0–999.
* `SECOND` — секунда. Возможные значения: 0–59.
* `MINUTE` — минута. Возможные значения: 0–59.
* `HOUR` — час. Возможные значения: 0–23.
* `DAY` — день месяца. Возможные значения: 1–31.
* `WEEK` — номер недели по ISO 8601. Возможные значения: 1–53.
* `MONTH` — номер месяца. Возможные значения: 1–12.
* `QUARTER` — квартал. Возможные значения: 1–4.
* `YEAR` — год.
* `EPOCH` — Unix-временная метка (секунды с 1970-01-01 00:00:00 UTC). Примечание: для `DateTime64` дробная часть секунды отбрасывается.
* `DOW` — день недели (совместимо с PostgreSQL). 0 = воскресенье, 6 = суббота.
* `DOY` — день года. Возможные значения: 1–366.
* `ISODOW` — день недели по ISO. 1 = понедельник, 7 = воскресенье.
* `ISOYEAR` — год нумерации недель по ISO 8601.
* `CENTURY` — век. Например, 2024 год относится к 21-му веку.
* `DECADE` — десятилетие (год, делённый на 10). Например, для 2024 года десятилетие равно 202.
* `MILLENNIUM` — тысячелетие. Например, 2024 год относится к 3-му тысячелетию.
* `TIMEZONE_HOUR` — знаковая часовая часть смещения UTC часового пояса операнда. Например, `+5:30` возвращает `5`, `-3:30` возвращает `-3`.
* `TIMEZONE_MINUTE` — знаковая минутная часть смещения UTC часового пояса операнда. Например, `+5:30` возвращает `30`, `-3:30` возвращает `-30`.

Параметр `part` регистронезависимый.

Параметр `date` задаёт значение для обработки. Поддерживаются типы [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md), [DateTime](../../sql-reference/data-types/datetime.md), [DateTime64](../../sql-reference/data-types/datetime64.md) и [Interval](../../sql-reference/data-types/special-data-types/interval.md). Если `date` имеет тип `Interval`, запрошенный `part` должен соответствовать хранимому виду интервала (например, `EXTRACT(DAY FROM INTERVAL 5 DAY)` допустим, а `EXTRACT(HOUR FROM INTERVAL 5 DAY)` будет отклонён, поскольку интервалы в ClickHouse поддерживают только один вид). Результат для операнда `Interval` имеет тип `Int64`.

Примеры:

```sql
SELECT EXTRACT(DAY FROM toDate('2017-06-15'));
SELECT EXTRACT(MONTH FROM toDate('2017-06-15'));
SELECT EXTRACT(YEAR FROM toDate('2017-06-15'));
SELECT EXTRACT(EPOCH FROM toDateTime('2024-01-15 12:30:45', 'UTC'));
SELECT EXTRACT(DOW FROM toDate('2024-01-15'));
SELECT EXTRACT(CENTURY FROM toDate('2024-01-01'));
SELECT EXTRACT(TIMEZONE_HOUR   FROM toDateTime('2024-01-15 12:00:00', 'Asia/Kolkata'));    -- 5
SELECT EXTRACT(TIMEZONE_MINUTE FROM toDateTime('2024-01-15 12:00:00', 'Asia/Kolkata'));    -- 30
SELECT EXTRACT(DAY   FROM INTERVAL 40 DAY);                                                -- 40
SELECT EXTRACT(MONTH FROM INTERVAL 7 MONTH);                                               -- 7
```

В следующем примере мы создаём таблицу и вставляем в неё значение типа `DateTime`.

```sql
CREATE TABLE test.Orders
(
    OrderId UInt64,
    OrderName String,
    OrderDate DateTime
) ENGINE = MergeTree
ORDER BY ();
```

```sql
INSERT INTO test.Orders VALUES (1, 'Jarlsberg Cheese', toDateTime('2008-10-11 13:23:44'));
```

```sql
SELECT
    toYear(OrderDate) AS OrderYear,
    toMonth(OrderDate) AS OrderMonth,
    toDayOfMonth(OrderDate) AS OrderDay,
    toHour(OrderDate) AS OrderHour,
    toMinute(OrderDate) AS OrderMinute,
    toSecond(OrderDate) AS OrderSecond
FROM test.Orders;
```

```text
┌─OrderYear─┬─OrderMonth─┬─OrderDay─┬─OrderHour─┬─OrderMinute─┬─OrderSecond─┐
│      2008 │         10 │       11 │        13 │          23 │          44 │
└───────────┴────────────┴──────────┴───────────┴─────────────┴─────────────┘
```

Больше примеров можно посмотреть в [тестах](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00619_extract.sql).

<div id="interval">
  ### INTERVAL
</div>

Создаёт значение типа [Interval](../../sql-reference/data-types/special-data-types/interval.md), которое используется в арифметических операциях со значениями типов [Date](../../sql-reference/data-types/date.md) и [DateTime](../../sql-reference/data-types/datetime.md).

Типы интервалов:

* `SECOND`
* `MINUTE`
* `HOUR`
* `DAY`
* `WEEK`
* `MONTH`
* `QUARTER`
* `YEAR`

При задании значения `INTERVAL` также можно использовать строковый литерал. Например, `INTERVAL 1 HOUR` идентичен `INTERVAL '1 hour'` или `INTERVAL '1' hour`.

:::tip
Интервалы разных типов нельзя комбинировать. Нельзя использовать выражения вида `INTERVAL 4 DAY 1 HOUR`. Указывайте интервалы в единицах, которые меньше или равны наименьшей единице интервала, например `INTERVAL 25 HOUR`. Можно использовать последовательные операции, как в примере ниже.
:::

Примеры:

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR;
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2020-11-03 22:09:50 │                                    2020-11-08 01:09:50 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL '4 day' + INTERVAL '3 hour';
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2020-11-03 22:12:10 │                                    2020-11-08 01:12:10 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL '4' day + INTERVAL '3' hour;
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay('4')), toIntervalHour('3'))─┐
│ 2020-11-03 22:33:19 │                                        2020-11-08 01:33:19 │
└─────────────────────┴────────────────────────────────────────────────────────────┘
```

:::note
Синтаксис `INTERVAL` или функция `addDays` всегда предпочтительнее. Простое сложение или вычитание (синтаксис вида `now() + ...`) не учитывает временные настройки, например переход на летнее время.
:::

Примеры:

```sql
SELECT toDateTime('2014-10-26 00:00:00', 'Asia/Istanbul') AS time, time + 60 * 60 * 24 AS time_plus_24_hours, time + toIntervalDay(1) AS time_plus_1_day;
```

```text
┌────────────────time─┬──time_plus_24_hours─┬─────time_plus_1_day─┐
│ 2014-10-26 00:00:00 │ 2014-10-26 23:00:00 │ 2014-10-27 00:00:00 │
└─────────────────────┴─────────────────────┴─────────────────────┘
```

**См. также**

* тип данных [Interval](../../sql-reference/data-types/special-data-types/interval.md)
* функции преобразования типов [toInterval](/ru/sql-reference/functions/type-conversion-functions#toIntervalYear)

<div id="date-time-addition">
  ### Сложение даты и времени
</div>

Значение [Date](../../sql-reference/data-types/date.md) или [Date32](../../sql-reference/data-types/date32.md) можно сложить со значением [Time](../../sql-reference/data-types/time.md) или [Time64](../../sql-reference/data-types/time64.md) с помощью оператора `+`. Результатом будет значение [DateTime](../../sql-reference/data-types/datetime.md) или [DateTime64](../../sql-reference/data-types/datetime64.md), представляющее дату с указанным временем суток. Операция коммутативна.

Тип результата зависит от типов операндов:

| Левый операнд | Правый операнд | Тип результата  |
| ------------- | -------------- | --------------- |
| `Date`        | `Time`         | `DateTime`      |
| `Date`        | `Time64(s)`    | `DateTime64(s)` |
| `Date32`      | `Time`         | `DateTime64(0)` |
| `Date32`      | `Time64(s)`    | `DateTime64(s)` |

:::note
В результате используется [часовой пояс сеанса](../../operations/settings/settings.md#session_timezone) (или часовой пояс сервера по умолчанию, если часовой пояс сеанса не задан). Настройка [`date_time_overflow_behavior`](../../operations/settings/settings-formats.md#date_time_overflow_behavior) определяет, что произойдет, если результат выходит за пределы допустимого диапазона.
:::

Примеры:

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime('14:30:25') AS dt, toTypeName(dt);
```

```text
┌──────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 14:30:25 │ DateTime       │
└─────────────────────┴────────────────┘
```

```sql
SELECT toDate('2024-07-15') + toTime64('14:30:25.123456', 6) AS dt, toTypeName(dt);
```

```text
┌─────────────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 14:30:25.123456 │ DateTime64(6)  │
└────────────────────────────┴────────────────┘
```

```sql
SELECT toTime64('23:59:59.999', 3) + toDate32('2024-07-15') AS dt, toTypeName(dt);
```

```text
┌──────────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 23:59:59.999 │ DateTime64(3)  │
└─────────────────────────┴────────────────┘
```

<div id="at-time-zone">
  ### AT TIME ZONE и AT LOCAL
</div>

Постфиксные операторы `AT TIME ZONE` и `AT LOCAL` переводят значение `DateTime` или `DateTime64` в другой часовой пояс. Это синтаксический сахар для уже существующей функции [`toTimeZone`](/ru/sql-reference/functions/date-time-functions#totimezone):

| Синтаксис                | Эквивалент                     |
| ------------------------ | ------------------------------ |
| `expr AT TIME ZONE zone` | `toTimeZone(expr, zone)`       |
| `expr AT LOCAL`          | `toTimeZone(expr, timeZone())` |

`zone` может быть любым константным строковым выражением, которое вычисляется в корректное имя часового пояса (например, `'America/Denver'`, `'UTC'` или `concat('America', '/', 'Denver')`). Поскольку `AT TIME ZONE` сводится к `toTimeZone`, для него действуют те же правила аргумента часового пояса: для неконстантных выражений, таких как ссылка на столбец, требуется [`allow_nonconst_timezone_arguments = 1`](../../operations/settings/settings.md#allow_nonconst_timezone_arguments).

`AT LOCAL` использует текущий [часовой пояс сеанса](../../operations/settings/settings.md#session_timezone) (или часовой пояс сервера по умолчанию, если для сеанса часовой пояс не задан). В таблицах `Distributed` параметр `session_timezone` должен быть задан явно; если он пуст, `timeZone()` локален для сегмента и не может использоваться как константный аргумент `toTimeZone`, что приводит к исключению `ILLEGAL_COLUMN`.

:::note
В отличие от PostgreSQL, где `timestamp without time zone AT TIME ZONE zone` сначала интерпретирует значение локального времени как относящееся к указанному часовому поясу, а затем выполняет преобразование, ClickHouse всегда сохраняет одну и ту же абсолютную точку во времени и меняет только метку часового пояса, используемую для отображения. Обе формы эквивалентны `toTimeZone` и не изменяют само значение временной метки.
:::

У `AT TIME ZONE` приоритет операций равен 13 (выше, чем у `*`/`/`/`%` с 12, и выше, чем у `+`/`-` с 11), как и в PostgreSQL. Это означает, что `a * ts AT TIME ZONE 'tz'` связывается как `a * (ts AT TIME ZONE 'tz')`, а `ts + interval AT TIME ZONE 'tz'` — как `ts + (interval AT TIME ZONE 'tz')`. Чтобы применить преобразование часового пояса после арифметической операции, используйте явные скобки:

```sql
-- Explicit parens required to add first, then convert timezone
SELECT (TIMESTAMP '2001-02-16 20:38:40' + INTERVAL 1 HOUR) AT TIME ZONE 'America/Denver';
-- Equivalent to:
SELECT toTimeZone(TIMESTAMP '2001-02-16 20:38:40' + INTERVAL 1 HOUR, 'America/Denver');
```

Примеры:

```sql
SET session_timezone = 'UTC';

SELECT TIMESTAMP '2001-02-16 20:38:40' AT TIME ZONE 'America/Denver';
```

```text
┌─toTimeZone(toDateTime('2001-02-16 20:38:40'), 'America/Denver')─┐
│ 2001-02-16 13:38:40                                              │
└──────────────────────────────────────────────────────────────────┘
```

```sql
SELECT TIMESTAMP '2001-02-16 20:38:40' AT LOCAL;
```

```text
┌─toTimeZone(toDateTime('2001-02-16 20:38:40'), timeZone())─┐
│ 2001-02-16 20:38:40                                        │
└────────────────────────────────────────────────────────────┘
```

**См. также**

* [`toTimeZone`](/ru/sql-reference/functions/date-time-functions#totimezone)
* [`timeZone`](/ru/sql-reference/functions/date-time-functions#timezone)

<div id="logical-and-operator">
  ## Оператор логического И
</div>

Синтаксис `SELECT a AND b` — выполняет логическую конъюнкцию `a` и `b` с помощью функции [and](/ru/sql-reference/functions/logical-functions#and).

<div id="logical-or-operator">
  ## Оператор логического OR
</div>

Синтаксис `SELECT a OR b` — вычисляет логическую дизъюнкцию между `a` и `b` с помощью функции [or](/ru/sql-reference/functions/logical-functions#or).

<div id="logical-negation-operator">
  ## Оператор логического отрицания
</div>

Синтаксис `SELECT NOT a` — вычисляет логическое отрицание `a` с помощью функции [not](/ru/sql-reference/functions/logical-functions#not).

<div id="conditional-operator">
  ## Условный оператор
</div>

`a ? b : c` — функция `if(a, b, c)`.

Примечание:

Условный оператор вычисляет значения b и c, затем проверяет, истинно ли условие a, и после этого возвращает соответствующее значение. Если `b` или `C` — функция [arrayJoin()](/ru/sql-reference/functions/array-join), каждая строка будет размножена независимо от условия &quot;a&quot;.

<div id="conditional-expression">
  ## Условное выражение
</div>

```sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    [ELSE c]
END
```

Если указан `x`, то используется функция `transform(x, [a, ...], [b, ...], c)`. В противном случае — `multiIf(a, b, ..., c)`.

Если в выражении отсутствует ветвь `ELSE c`, значением по умолчанию будет `NULL`.

Функция `transform` не работает с `NULL`.

<div id="concatenation-operator">
  ## Оператор конкатенации
</div>

`s1 || s2` – Функция `concat(s1, s2)`.

<div id="lambda-creation-operator">
  ## Оператор создания lambda
</div>

`x -> expr` — функция `lambda(x, expr)`.

Следующие операторы не имеют приоритета, так как это скобки:

<div id="array-creation-operator">
  ## Оператор создания Array
</div>

`[x1, ...]` — функция `array(x1, ...)`.

<div id="tuple-creation-operator">
  ## Оператор создания Tuple
</div>

`(x1, x2, ...)` — функция `tuple(x2, x2, ...)`.

<div id="associativity">
  ## Ассоциативность
</div>

Все бинарные операторы имеют левую ассоциативность. Например, `1 + 2 + 3` преобразуется в `plus(plus(1, 2), 3)`.
Иногда это работает не так, как ожидается. Например, `SELECT 4 > 2 > 3` даст результат 0.

Для повышения эффективности функции `and` и `or` принимают любое число аргументов. Соответствующие цепочки операторов `AND` и `OR` преобразуются в один вызов этих функций.

<div id="checking-for-null">
  ## Проверка на `NULL`
</div>

ClickHouse поддерживает операторы `IS NULL` и `IS NOT NULL`.

<div id="is_null">
  ### IS NULL
</div>

* Для значений типа [Nullable](../../sql-reference/data-types/nullable.md) оператор `IS NULL` возвращает:
  * `1`, если значение равно `NULL`.
  * `0` в остальных случаях.
* Для всех остальных значений оператор `IS NULL` всегда возвращает `0`.

Это можно оптимизировать, включив настройку [optimize&#95;functions&#95;to&#95;subcolumns](/ru/operations/settings/settings#optimize_functions_to_subcolumns). При `optimize_functions_to_subcolumns = 1` функция считывает только подстолбец [null](../../sql-reference/data-types/nullable.md#finding-null) вместо чтения и обработки данных всего столбца. Запрос `SELECT n IS NULL FROM table` преобразуется в `SELECT n.null FROM TABLE`.

{/* */ }

```sql
SELECT x+100 FROM t_null WHERE y IS NULL
```

```text
┌─plus(x, 100)─┐
│          101 │
└──────────────┘
```

<div id="is_not_null">
  ### IS NOT NULL
</div>

* Для значений типа [Nullable](../../sql-reference/data-types/nullable.md) оператор `IS NOT NULL` возвращает:
  * `0`, если значение равно `NULL`.
  * `1` во всех остальных случаях.
* Для остальных значений оператор `IS NOT NULL` всегда возвращает `1`.

{/* */ }

```sql
SELECT * FROM t_null WHERE y IS NOT NULL
```

```text
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```

Можно оптимизировать, включив настройку [optimize&#95;functions&#95;to&#95;subcolumns](/ru/operations/settings/settings#optimize_functions_to_subcolumns). При `optimize_functions_to_subcolumns = 1` функция считывает только подстолбец [null](../../sql-reference/data-types/nullable.md#finding-null), а не читает и обрабатывает данные всего столбца. Запрос `SELECT n IS NOT NULL FROM table` преобразуется в `SELECT NOT n.null FROM TABLE`.

<div id="checking-boolean-values">
  ## Проверка булевых значений
</div>

ClickHouse поддерживает операторы `IS TRUE`, `IS FALSE`, `IS UNKNOWN`, `IS NOT TRUE`, `IS NOT FALSE` и `IS NOT UNKNOWN`.
Они используются с выражениями [Bool](../../sql-reference/data-types/boolean.md) и `Nullable(Bool)`.

* `expr IS TRUE` возвращает `1` только в том случае, если `expr` имеет значение `true`.
* `expr IS FALSE` возвращает `1` только в том случае, если `expr` имеет значение `false`.
* `expr IS UNKNOWN` возвращает `1` только в том случае, если `expr` имеет значение `NULL`.
* `expr IS NOT TRUE` возвращает `1`, если `expr` имеет значение `false` или `NULL`.
* `expr IS NOT FALSE` возвращает `1`, если `expr` имеет значение `true` или `NULL`.
* `expr IS NOT UNKNOWN` возвращает `1`, если `expr` не равно `NULL`.

Для булевых выражений `IS UNKNOWN` эквивалентно `IS NULL`, а `IS NOT UNKNOWN` — `IS NOT NULL`.

{/* */ }

```sql
CREATE TABLE t_bool (x Nullable(Bool)) ENGINE = Memory;
INSERT INTO t_bool VALUES (true), (false), (NULL);

SELECT
    x,
    x IS TRUE,
    x IS FALSE,
    x IS UNKNOWN,
    x IS NOT TRUE,
    x IS NOT FALSE,
    x IS NOT UNKNOWN
FROM t_bool;
```