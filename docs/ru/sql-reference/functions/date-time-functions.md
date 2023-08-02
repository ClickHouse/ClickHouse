---
sidebar_position: 39
sidebar_label: "Функции для работы с датами и временем"
---

# Функции для работы с датами и временем {#funktsii-dlia-raboty-s-datami-i-vremenem}

Поддержка часовых поясов

Все функции по работе с датой и временем, для которых это имеет смысл, могут принимать второй, необязательный аргумент - имя часового пояса. Пример: Asia/Yekaterinburg. В этом случае, они используют не локальный часовой пояс (по умолчанию), а указанный.

``` sql
SELECT
    toDateTime('2016-06-15 23:00:00') AS time,
    toDate(time) AS date_local,
    toDate(time, 'Asia/Yekaterinburg') AS date_yekat,
    toString(time, 'US/Samoa') AS time_samoa
```

``` text
┌────────────────time─┬─date_local─┬─date_yekat─┬─time_samoa──────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-16 │ 2016-06-15 09:00:00 │
└─────────────────────┴────────────┴────────────┴─────────────────────┘
```

## timeZone {#timezone}

Возвращает часовой пояс сервера.
Если функция вызывается в контексте распределенной таблицы, то она генерирует обычный столбец со значениями, актуальными для каждого шарда. Иначе возвращается константа.

**Синтаксис**

``` sql
timeZone()
```

Синоним: `timezone`.

**Возвращаемое значение**

-   Часовой пояс.

Тип: [String](../../sql-reference/data-types/string.md).

## toTimeZone {#totimezone}

Переводит дату или дату с временем в указанный часовой пояс. Часовой пояс - это атрибут типов `Date` и `DateTime`. Внутреннее значение (количество секунд) поля таблицы или результирующего столбца не изменяется, изменяется тип поля и, соответственно, его текстовое отображение.

**Синтаксис**

``` sql
toTimezone(value, timezone)
```

Синоним: `toTimezone`.

**Аргументы**

-   `value` — время или дата с временем. [DateTime64](../../sql-reference/data-types/datetime64.md).
-   `timezone` — часовой пояс для возвращаемого значения. [String](../../sql-reference/data-types/string.md). Этот аргумент является константой, потому что `toTimezone` изменяет часовой пояс столбца (часовой пояс является атрибутом типов `DateTime*`).

**Возвращаемое значение**

-   Дата с временем.

Тип: [DateTime](../../sql-reference/data-types/datetime.md).

**Пример**

Запрос:

```sql
SELECT toDateTime('2019-01-01 00:00:00', 'UTC') AS time_utc,
    toTypeName(time_utc) AS type_utc,
    toInt32(time_utc) AS int32utc,
    toTimeZone(time_utc, 'Asia/Yekaterinburg') AS time_yekat,
    toTypeName(time_yekat) AS type_yekat,
    toInt32(time_yekat) AS int32yekat,
    toTimeZone(time_utc, 'US/Samoa') AS time_samoa,
    toTypeName(time_samoa) AS type_samoa,
    toInt32(time_samoa) AS int32samoa
FORMAT Vertical;
```

Результат:

```text
Row 1:
──────
time_utc:   2019-01-01 00:00:00
type_utc:   DateTime('UTC')
int32utc:   1546300800
time_yekat: 2019-01-01 05:00:00
type_yekat: DateTime('Asia/Yekaterinburg')
int32yekat: 1546300800
time_samoa: 2018-12-31 13:00:00
type_samoa: DateTime('US/Samoa')
int32samoa: 1546300800
```

`toTimeZone(time_utc, 'Asia/Yekaterinburg')` изменяет тип `DateTime('UTC')` в `DateTime('Asia/Yekaterinburg')`. Значение (unix-время) 1546300800 остается неизменным, но текстовое отображение (результат функции toString()) меняется `time_utc:   2019-01-01 00:00:00` в `time_yekat: 2019-01-01 05:00:00`.

## timeZoneOf {#timezoneof}

Возвращает название часового пояса для значений типа [DateTime](../../sql-reference/data-types/datetime.md) и [DateTime64](../../sql-reference/data-types/datetime64.md).

**Синтаксис**

``` sql
timeZoneOf(value)
```

Синоним: `timezoneOf`.

**Аргументы**

-   `value` — Дата с временем. [DateTime](../../sql-reference/data-types/datetime.md) или [DateTime64](../../sql-reference/data-types/datetime64.md).

**Возвращаемое значение**

-   Название часового пояса.

Тип: [String](../../sql-reference/data-types/string.md).

**Пример**

Запрос:
``` sql
SELECT timezoneOf(now());
```

Результат:
``` text
┌─timezoneOf(now())─┐
│ Etc/UTC           │
└───────────────────┘
```

## timeZoneOffset {#timezoneoffset}

Возвращает смещение часового пояса в секундах от [UTC](https://ru.wikipedia.org/wiki/Всемирное_координированное_время). Функция учитывает [летнее время](https://ru.wikipedia.org/wiki/Летнее_время) и исторические изменения часовых поясов, которые действовали на указанную дату.
Для вычисления смещения используется информация из [базы данных IANA](https://www.iana.org/time-zones).

**Синтаксис**

``` sql
timeZoneOffset(value)
```

Синоним: `timezoneOffset`.

**Аргументы**

-   `value` — Дата с временем. [DateTime](../../sql-reference/data-types/datetime.md) or [DateTime64](../../sql-reference/data-types/datetime64.md).

**Возвращаемое значение**

-   Смещение в секундах от UTC.

Тип: [Int32](../../sql-reference/data-types/int-uint.md).

**Пример**

Запрос:

``` sql
SELECT toDateTime('2021-04-21 10:20:30', 'Europe/Moscow') AS Time, toTypeName(Time) AS Type,
       timeZoneOffset(Time) AS Offset_in_seconds, (Offset_in_seconds / 3600) AS Offset_in_hours;
```

Результат:

``` text
┌────────────────Time─┬─Type──────────────────────┬─Offset_in_seconds─┬─Offset_in_hours─┐
│ 2021-04-21 10:20:30 │ DateTime('Europe/Moscow') │             10800 │               3 │
└─────────────────────┴───────────────────────────┴───────────────────┴─────────────────┘
```

## toYear {#toyear}

Переводит дату или дату-с-временем в число типа UInt16, содержащее номер года (AD).

Синоним: `YEAR`.

## toQuarter {#toquarter}

Переводит дату или дату-с-временем в число типа UInt8, содержащее номер квартала.

Синоним: `QUARTER`.

## toMonth {#tomonth}

Переводит дату или дату-с-временем в число типа UInt8, содержащее номер месяца (1-12).

Синоним: `MONTH`.

## toDayOfYear {#todayofyear}

Переводит дату или дату-с-временем в число типа UInt16, содержащее номер дня года (1-366).

Синоним: `DAYOFYEAR`.

## toDayOfMonth {#todayofmonth}

Переводит дату или дату-с-временем в число типа UInt8, содержащее номер дня в месяце (1-31).

Синонимы: `DAYOFMONTH`, `DAY`.

## toDayOfWeek {#todayofweek}

Переводит дату или дату-с-временем в число типа UInt8, содержащее номер дня в неделе (понедельник - 1, воскресенье - 7).

Синоним: `DAYOFWEEK`.

## toHour {#tohour}

Переводит дату-с-временем в число типа UInt8, содержащее номер часа в сутках (0-23).
Функция исходит из допущения, что перевод стрелок вперёд, если осуществляется, то на час, в два часа ночи, а перевод стрелок назад, если осуществляется, то на час, в три часа ночи (что, в общем, не верно - даже в Москве два раза перевод стрелок был осуществлён в другое время).

Синоним: `HOUR`.

## toMinute {#tominute}

Переводит дату-с-временем в число типа UInt8, содержащее номер минуты в часе (0-59).

Синоним: `MINUTE`.

## toSecond {#tosecond}

Переводит дату-с-временем в число типа UInt8, содержащее номер секунды в минуте (0-59).
Секунды координации не учитываются.

Синоним: `SECOND`.

## toUnixTimestamp {#to-unix-timestamp}

Переводит дату-с-временем в число типа UInt32 -- Unix Timestamp (https://en.wikipedia.org/wiki/Unix_time).
Для аргумента String, строка конвертируется в дату и время в соответствии с часовым поясом (необязательный второй аргумент, часовой пояс сервера используется по умолчанию).

**Синтаксис**

``` sql
toUnixTimestamp(datetime)
toUnixTimestamp(str, [timezone])
```

**Возвращаемое значение**

-   Возвращает Unix Timestamp.

Тип: `UInt32`.

**Пример**

Запрос:

``` sql
SELECT toUnixTimestamp('2017-11-05 08:07:47', 'Asia/Tokyo') AS unix_timestamp;
```

Результат:

``` text
┌─unix_timestamp─┐
│     1509836867 │
└────────────────┘
```

    :::note "Attention"
    `Date` или `DateTime` это возвращаемый тип функций `toStartOf*`, который описан ниже. Несмотря на то, что эти функции могут принимать `DateTime64` в качестве аргумента, если переданное значение типа `DateTime64` выходит за пределы нормального диапазона (с 1900 по 2299 год), то это даст неверный результат.
    :::
## toStartOfYear {#tostartofyear}

Округляет дату или дату-с-временем вниз до первого дня года.
Возвращается дата.

## toStartOfISOYear {#tostartofisoyear}

Округляет дату или дату-с-временем вниз до первого дня ISO года. Возвращается дата.
Начало ISO года отличается от начала обычного года, потому что в соответствии с [ISO 8601:1988](https://en.wikipedia.org/wiki/ISO_8601) первая неделя года - это неделя с четырьмя или более днями в этом году.

1 Января 2017 г. - воскресение, т.е. первая ISO неделя 2017 года началась в понедельник 2 января, поэтому 1 января 2017 это 2016 ISO-год, который начался 2016-01-04.

```sql
SELECT toStartOfISOYear(toDate('2017-01-01')) AS ISOYear20170101;
```

```text
┌─ISOYear20170101─┐
│      2016-01-04 │
└─────────────────┘
```

## toStartOfQuarter {#tostartofquarter}

Округляет дату или дату-с-временем вниз до первого дня квартала.
Первый день квартала - это одно из 1 января, 1 апреля, 1 июля, 1 октября.
Возвращается дата.

## toStartOfMonth {#tostartofmonth}

Округляет дату или дату-с-временем вниз до первого дня месяца.
Возвращается дата.

    :::note "Attention"
    Возвращаемое значение для некорректных дат зависит от реализации. ClickHouse может вернуть нулевую дату, выбросить исключение, или выполнить «естественное» перетекание дат между месяцами.
    :::
## toMonday {#tomonday}

Округляет дату или дату-с-временем вниз до ближайшего понедельника.
Возвращается дата.

## toStartOfWeek(t[,mode]) {#tostartofweek}

Округляет дату или дату со временем до ближайшего воскресенья или понедельника в соответствии с mode.
Возвращается дата.
Аргумент mode работает точно так же, как аргумент mode [toWeek()](#toweek). Если аргумент mode опущен, то используется режим 0.

## toStartOfDay {#tostartofday}

Округляет дату-с-временем вниз до начала дня. Возвращается дата-с-временем.

## toStartOfHour {#tostartofhour}

Округляет дату-с-временем вниз до начала часа.

## toStartOfMinute {#tostartofminute}

Округляет дату-с-временем вниз до начала минуты.

## toStartOfSecond {#tostartofsecond}

Отсекает доли секунды.

**Синтаксис**

``` sql
toStartOfSecond(value, [timezone])
```

**Аргументы**

-   `value` — дата и время. [DateTime64](../data-types/datetime64.md).
-   `timezone` — [часовой пояс](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) для возвращаемого значения (необязательно). Если параметр не задан, используется часовой пояс параметра `value`. [String](../data-types/string.md).

**Возвращаемое значение**

-   Входное значение с отсеченными долями секунды.

Тип: [DateTime64](../data-types/datetime64.md).

**Примеры**

Пример без часового пояса:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999', 3) AS dt64 SELECT toStartOfSecond(dt64);
```

Результат:

``` text
┌───toStartOfSecond(dt64)─┐
│ 2020-01-01 10:20:30.000 │
└─────────────────────────┘
```

Пример с часовым поясом:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999', 3) AS dt64 SELECT toStartOfSecond(dt64, 'Europe/Moscow');
```

Результат:

``` text
┌─toStartOfSecond(dt64, 'Europe/Moscow')─┐
│                2020-01-01 13:20:30.000 │
└────────────────────────────────────────┘
```

**Смотрите также**

-   Часовая зона сервера, конфигурационный параметр [timezone](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone).


## toStartOfFiveMinutes {#tostartoffiveminutes}

Округляет дату-с-временем вниз до начала пятиминутного интервала.

## toStartOfTenMinutes {#tostartoftenminutes}

Округляет дату-с-временем вниз до начала десятиминутного интервала.

## toStartOfFifteenMinutes {#tostartoffifteenminutes}

Округляет дату-с-временем вниз до начала пятнадцатиминутного интервала.

## toStartOfInterval(time_or_data, INTERVAL x unit \[, time_zone\]) {#tostartofintervaltime-or-data-interval-x-unit-time-zone}

Обобщение остальных функций `toStartOf*`. Например,
`toStartOfInterval(t, INTERVAL 1 year)` возвращает то же самое, что и `toStartOfYear(t)`,
`toStartOfInterval(t, INTERVAL 1 month)` возвращает то же самое, что и `toStartOfMonth(t)`,
`toStartOfInterval(t, INTERVAL 1 day)` возвращает то же самое, что и `toStartOfDay(t)`,
`toStartOfInterval(t, INTERVAL 15 minute)` возвращает то же самое, что и `toStartOfFifteenMinutes(t)`, и т.п.

## toTime {#totime}

Переводит дату-с-временем на некоторую фиксированную дату, сохраняя при этом время.

## toRelativeYearNum {#torelativeyearnum}

Переводит дату-с-временем или дату в номер года, начиная с некоторого фиксированного момента в прошлом.

## toRelativeQuarterNum {#torelativequarternum}

Переводит дату-с-временем или дату в номер квартала, начиная с некоторого фиксированного момента в прошлом.

## toRelativeMonthNum {#torelativemonthnum}

Переводит дату-с-временем или дату в номер месяца, начиная с некоторого фиксированного момента в прошлом.

## toRelativeWeekNum {#torelativeweeknum}

Переводит дату-с-временем или дату в номер недели, начиная с некоторого фиксированного момента в прошлом.

## toRelativeDayNum {#torelativedaynum}

Переводит дату-с-временем или дату в номер дня, начиная с некоторого фиксированного момента в прошлом.

## toRelativeHourNum {#torelativehournum}

Переводит дату-с-временем в номер часа, начиная с некоторого фиксированного момента в прошлом.

## toRelativeMinuteNum {#torelativeminutenum}

Переводит дату-с-временем в номер минуты, начиная с некоторого фиксированного момента в прошлом.

## toRelativeSecondNum {#torelativesecondnum}

Переводит дату-с-временем в номер секунды, начиная с некоторого фиксированного момента в прошлом.

## toISOYear {#toisoyear}

Переводит дату-с-временем или дату в число типа UInt16, содержащее номер ISO года. ISO год отличается от обычного года, потому что в соответствии с [ISO 8601:1988](https://en.wikipedia.org/wiki/ISO_8601) ISO год начинается необязательно первого января.

**Пример**

Запрос:

```sql
SELECT
    toDate('2017-01-01') AS date,
    toYear(date),
    toISOYear(date)
```

Результат:

```text
┌───────date─┬─toYear(toDate('2017-01-01'))─┬─toISOYear(toDate('2017-01-01'))─┐
│ 2017-01-01 │                         2017 │                            2016 │
└────────────┴──────────────────────────────┴─────────────────────────────────┘
```

## toISOWeek {#toisoweek}

Переводит дату-с-временем или дату в число типа UInt8, содержащее номер ISO недели.
Начало ISO года отличается от начала обычного года, потому что в соответствии с [ISO 8601:1988](https://en.wikipedia.org/wiki/ISO_8601) первая неделя года - это неделя с четырьмя или более днями в этом году.

1 Января 2017 г. - воскресение, т.е. первая ISO неделя 2017 года началась в понедельник 2 января, поэтому 1 января 2017 это последняя неделя 2016 года.

**Пример**

Запрос:

```sql
SELECT
    toISOWeek(toDate('2017-01-01')) AS ISOWeek20170101,
    toISOWeek(toDate('2017-01-02')) AS ISOWeek20170102
```

Результат:

```text
┌─ISOWeek20170101─┬─ISOWeek20170102─┐
│              52 │               1 │
└─────────────────┴─────────────────┘
```

## toWeek(date\[, mode\]\[, timezone\]) {#toweek}
Переводит дату-с-временем или дату в число UInt8, содержащее номер недели. Второй аргументам mode задает режим, начинается ли неделя с воскресенья или с понедельника и должно ли возвращаемое значение находиться в диапазоне от 0 до 53 или от 1 до 53. Если аргумент mode опущен, то используется режим 0.

`toISOWeek() ` эквивалентно `toWeek(date,3)`.

Описание режимов (mode):

| Mode | Первый день недели | Диапазон |  Неделя 1 это первая неделя … |
| ----------- | -------- | -------- | ------------------ |
|0|Воскресенье|0-53|с воскресеньем в этом году
|1|Понедельник|0-53|с 4-мя или более днями в этом году
|2|Воскресенье|1-53|с воскресеньем в этом году
|3|Понедельник|1-53|с 4-мя или более днями в этом году
|4|Воскресенье|0-53|с 4-мя или более днями в этом году
|5|Понедельник|0-53|с понедельником в этом году
|6|Воскресенье|1-53|с 4-мя или более днями в этом году
|7|Понедельник|1-53|с понедельником в этом году
|8|Воскресенье|1-53|содержащая 1 января
|9|Понедельник|1-53|содержащая 1 января

Для режимов со значением «с 4 или более днями в этом году» недели нумеруются в соответствии с ISO 8601:1988:

- Если неделя, содержащая 1 января, имеет 4 или более дней в новом году, это неделя 1.

- В противном случае это последняя неделя предыдущего года, а следующая неделя - неделя 1.

Для режимов со значением «содержит 1 января», неделя 1 – это неделя содержащая 1 января. Не имеет значения, сколько дней в новом году содержала неделя, даже если она содержала только один день.

**Пример**

Запрос:

```sql
SELECT toDate('2016-12-27') AS date, toWeek(date) AS week0, toWeek(date,1) AS week1, toWeek(date,9) AS week9;
```

Результат:

```text
┌───────date─┬─week0─┬─week1─┬─week9─┐
│ 2016-12-27 │    52 │    52 │     1 │
└────────────┴───────┴───────┴───────┘
```

## toYearWeek(date[,mode]) {#toyearweek}
Возвращает год и неделю для даты. Год в результате может отличаться от года в аргументе даты для первой и последней недели года.

Аргумент mode работает точно так же, как аргумент mode [toWeek()](#toweek). Если mode не задан, используется режим 0.

`toISOYear() ` эквивалентно `intDiv(toYearWeek(date,3),100)`.

**Пример**

Запрос:

```sql
SELECT toDate('2016-12-27') AS date, toYearWeek(date) AS yearWeek0, toYearWeek(date,1) AS yearWeek1, toYearWeek(date,9) AS yearWeek9;
```

Результат:

```text
┌───────date─┬─yearWeek0─┬─yearWeek1─┬─yearWeek9─┐
│ 2016-12-27 │    201652 │    201652 │    201701 │
└────────────┴───────────┴───────────┴───────────┘
```

## date_trunc {#date_trunc}

Отсекает от даты и времени части, меньшие чем указанная часть.

**Синтаксис**

``` sql
date_trunc(unit, value[, timezone])
```

Синоним: `dateTrunc`.

**Аргументы**

-   `unit` — единица измерения времени, в которой задана отсекаемая часть. [String Literal](../syntax.md#syntax-string-literal).
    Возможные значения:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

-   `value` — дата и время. [DateTime](../../sql-reference/data-types/datetime.md) или [DateTime64](../../sql-reference/data-types/datetime64.md).
-   `timezone` — [часовой пояс](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) для возвращаемого значения (необязательно). Если параметр не задан, используется часовой пояс параметра `value`. [String](../../sql-reference/data-types/string.md)

**Возвращаемое значение**

-   Дата и время, отсеченные до указанной части.

Тип: [Datetime](../../sql-reference/data-types/datetime.md).

**Примеры**

Запрос без указания часового пояса:

``` sql
SELECT now(), date_trunc('hour', now());
```

Результат:

``` text
┌───────────────now()─┬─date_trunc('hour', now())─┐
│ 2020-09-28 10:40:45 │       2020-09-28 10:00:00 │
└─────────────────────┴───────────────────────────┘
```

Запрос с указанием часового пояса:

```sql
SELECT now(), date_trunc('hour', now(), 'Europe/Moscow');
```

Результат:

```text
┌───────────────now()─┬─date_trunc('hour', now(), 'Europe/Moscow')─┐
│ 2020-09-28 10:46:26 │                        2020-09-28 13:00:00 │
└─────────────────────┴────────────────────────────────────────────┘
```

**Смотрите также**

-   [toStartOfInterval](#tostartofintervaltime-or-data-interval-x-unit-time-zone)

## date\_add {#date_add}

Добавляет интервал времени или даты к указанной дате или дате со временем.

**Синтаксис**

``` sql
date_add(unit, value, date)
```

Синонимы: `dateAdd`, `DATE_ADD`.

**Аргументы**

-   `unit` — единица измерения времени, в которой задан интервал для добавления. [String](../../sql-reference/data-types/string.md).
    Возможные значения:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

-   `value` — значение интервала для добавления. [Int](../../sql-reference/data-types/int-uint.md).
-   `date` — дата или дата со временем, к которой добавляется `value`. [Date](../../sql-reference/data-types/date.md) или [DateTime](../../sql-reference/data-types/datetime.md).

**Возвращаемое значение**

Дата или дата со временем, полученная в результате добавления `value`, выраженного в `unit`, к `date`.

Тип: [Date](../../sql-reference/data-types/date.md) или [DateTime](../../sql-reference/data-types/datetime.md).

**Пример**

Запрос:

```sql
SELECT date_add(YEAR, 3, toDate('2018-01-01'));
```

Результат:

```text
┌─plus(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                    2021-01-01 │
└───────────────────────────────────────────────┘
```

## date\_diff {#date_diff}

Вычисляет разницу между двумя значениями дат или дат со временем.

**Синтаксис**

``` sql
date_diff('unit', startdate, enddate, [timezone])
```

Синонимы: `dateDiff`, `DATE_DIFF`.

**Аргументы**

-   `unit` — единица измерения времени, в которой будет выражено возвращаемое значение функции. [String](../../sql-reference/data-types/string.md).
    Возможные значения:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

-   `startdate` — первая дата или дата со временем, которая вычитается из `enddate`. [Date](../../sql-reference/data-types/date.md) или [DateTime](../../sql-reference/data-types/datetime.md).

-   `enddate` — вторая дата или дата со временем, из которой вычитается `startdate`. [Date](../../sql-reference/data-types/date.md) или [DateTime](../../sql-reference/data-types/datetime.md).

-   `timezone` — [часовой пояс](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) (необязательно). Если этот аргумент указан, то он применяется как для `startdate`, так и для `enddate`. Если этот аргумент не указан, то используются часовые пояса аргументов `startdate` и `enddate`. Если часовые пояса аргументов `startdate` и `enddate` не совпадают, то результат не определен. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

Разница между `enddate` и `startdate`, выраженная в `unit`.

Тип: [Int](../../sql-reference/data-types/int-uint.md).

**Пример**

Запрос:

``` sql
SELECT dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'));
```

Результат:

``` text
┌─dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'))─┐
│                                                                                     25 │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

## date\_sub {#date_sub}

Вычитает интервал времени или даты из указанной даты или даты со временем.

**Синтаксис**

``` sql
date_sub(unit, value, date)
```

Синонимы: `dateSub`, `DATE_SUB`.

**Аргументы**

-   `unit` — единица измерения времени, в которой задан интервал для вычитания. [String](../../sql-reference/data-types/string.md).
    Возможные значения:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

-   `value` — значение интервала для вычитания. [Int](../../sql-reference/data-types/int-uint.md).
-   `date` — дата или дата со временем, из которой вычитается `value`. [Date](../../sql-reference/data-types/date.md) или [DateTime](../../sql-reference/data-types/datetime.md).

**Возвращаемое значение**

Дата или дата со временем, полученная в результате вычитания `value`, выраженного в `unit`, из `date`.

Тип: [Date](../../sql-reference/data-types/date.md) или [DateTime](../../sql-reference/data-types/datetime.md).

**Пример**

Запрос:

``` sql
SELECT date_sub(YEAR, 3, toDate('2018-01-01'));
```

Результат:

``` text
┌─minus(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                     2015-01-01 │
└────────────────────────────────────────────────┘
```

## timestamp\_add {#timestamp_add}

Добавляет интервал времени к указанной дате или дате со временем.

**Синтаксис**

``` sql
timestamp_add(date, INTERVAL value unit)
```

Синонимы: `timeStampAdd`, `TIMESTAMP_ADD`.

**Аргументы**

-   `date` — дата или дата со временем. [Date](../../sql-reference/data-types/date.md) или [DateTime](../../sql-reference/data-types/datetime.md).
-   `value` — значение интервала для добавления. [Int](../../sql-reference/data-types/int-uint.md).
-   `unit` — единица измерения времени, в которой задан интервал для добавления. [String](../../sql-reference/data-types/string.md).
    Возможные значения:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

**Возвращаемое значение**

Дата или дата со временем, полученная в результате добавления `value`, выраженного в `unit`, к `date`.

Тип: [Date](../../sql-reference/data-types/date.md) или [DateTime](../../sql-reference/data-types/datetime.md).

**Пример**

Запрос:

```sql
select timestamp_add(toDate('2018-01-01'), INTERVAL 3 MONTH);
```

Результат:

```text
┌─plus(toDate('2018-01-01'), toIntervalMonth(3))─┐
│                                     2018-04-01 │
└────────────────────────────────────────────────┘
```

## timestamp\_sub {#timestamp_sub}

Вычитает интервал времени из указанной даты или даты со временем.

**Синтакис**

``` sql
timestamp_sub(unit, value, date)
```

Синонимы: `timeStampSub`, `TIMESTAMP_SUB`.

**Аргументы**

-   `unit` — единица измерения времени, в которой задан интервал для вычитания. [String](../../sql-reference/data-types/string.md).
    Возможные значения:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

-   `value` — значение интервала для вычитания. [Int](../../sql-reference/data-types/int-uint.md).
-   `date` — дата или дата со временем. [Date](../../sql-reference/data-types/date.md) или [DateTime](../../sql-reference/data-types/datetime.md).

**Возвращаемое значение**

Дата или дата со временем, полученная в результате вычитания `value`, выраженного в `unit`, из `date`.

Тип: [Date](../../sql-reference/data-types/date.md) или [DateTime](../../sql-reference/data-types/datetime.md).

**Пример**

Запрос:

```sql
select timestamp_sub(MONTH, 5, toDateTime('2018-12-18 01:02:03'));
```

Результат:

```text
┌─minus(toDateTime('2018-12-18 01:02:03'), toIntervalMonth(5))─┐
│                                          2018-07-18 01:02:03 │
└──────────────────────────────────────────────────────────────┘
```

## now {#now}

Возвращает текущую дату и время.

**Синтаксис**

``` sql
now([timezone])
```

**Параметры**

-   `timezone` — [часовой пояс](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) для возвращаемого значения (необязательно). [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   Текущие дата и время.

Тип: [Datetime](../../sql-reference/data-types/datetime.md).

**Пример**

Запрос без указания часового пояса:

``` sql
SELECT now();
```

Результат:

``` text
┌───────────────now()─┐
│ 2020-10-17 07:42:09 │
└─────────────────────┘
```

Запрос с указанием часового пояса:

``` sql
SELECT now('Europe/Moscow');
```

Результат:

``` text
┌─now('Europe/Moscow')─┐
│  2020-10-17 10:42:23 │
└──────────────────────┘
```

## today {#today}

Возвращает текущую дату на момент выполнения запроса. Функция не требует аргументов.
То же самое, что toDate(now())

## yesterday {#yesterday}

Возвращает вчерашнюю дату на момент выполнения запроса.
Делает то же самое, что today() - 1. Функция не требует аргументов.

## timeSlot {#timeslot}

Округляет время до получаса.
Эта функция является специфичной для Яндекс.Метрики, так как полчаса - минимальное время, для которого, если соседние по времени хиты одного посетителя на одном счётчике отстоят друг от друга строго более, чем на это время, визит может быть разбит на два визита. То есть, кортежи (номер счётчика, идентификатор посетителя, тайм-слот) могут использоваться для поиска хитов, входящий в соответствующий визит.

## timeSlots(StartTime, Duration,\[, Size\]) {#timeslotsstarttime-duration-size}
Для интервала, начинающегося в `StartTime` и длящегося `Duration` секунд, возвращает массив моментов времени, кратных `Size`. Параметр `Size` указывать необязательно, по умолчанию он равен 1800 секундам (30 минутам) - необязательный параметр.
Данная функция может использоваться, например, для анализа количества просмотров страницы за соответствующую сессию. 
Аргумент `StartTime` может иметь тип `DateTime` или `DateTime64`. В случае, если используется `DateTime`, аргументы `Duration` и `Size` должны иметь тип `UInt32`; Для DateTime64 они должны быть типа `Decimal64`.  
Возвращает массив DateTime/DateTime64 (тип будет совпадать с типом параметра ’StartTime’). Для DateTime64 масштаб(scale) возвращаемой величины может отличаться от масштаба фргумента ’StartTime’ --- результат будет иметь наибольший масштаб среди всех данных аргументов.

Пример использования:
```sql
SELECT timeSlots(toDateTime('2012-01-01 12:20:00'), toUInt32(600));
SELECT timeSlots(toDateTime('1980-12-12 21:01:02', 'UTC'), toUInt32(600), 299);
SELECT timeSlots(toDateTime64('1980-12-12 21:01:02.1234', 4, 'UTC'), toDecimal64(600.1, 1), toDecimal64(299, 0));
```
``` text
┌─timeSlots(toDateTime('2012-01-01 12:20:00'), toUInt32(600))─┐
│ ['2012-01-01 12:00:00','2012-01-01 12:30:00']               │
└─────────────────────────────────────────────────────────────┘
┌─timeSlots(toDateTime('1980-12-12 21:01:02', 'UTC'), toUInt32(600), 299)─┐
│ ['1980-12-12 20:56:13','1980-12-12 21:01:12','1980-12-12 21:06:11']     │
└─────────────────────────────────────────────────────────────────────────┘
┌─timeSlots(toDateTime64('1980-12-12 21:01:02.1234', 4, 'UTC'), toDecimal64(600.1, 1), toDecimal64(299, 0))─┐
│ ['1980-12-12 20:56:13.0000','1980-12-12 21:01:12.0000','1980-12-12 21:06:11.0000']                        │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## toYYYYMM

Переводит дату или дату со временем в число типа UInt32, содержащее номер года и месяца (YYYY * 100 + MM).

## toYYYYMMDD

Переводит дату или дату со временем в число типа UInt32, содержащее номер года, месяца и дня (YYYY * 10000 + MM * 100 + DD).

## toYYYYMMDDhhmmss

Переводит дату или дату со временем в число типа UInt64 содержащее номер года, месяца, дня и время (YYYY * 10000000000 + MM * 100000000 + DD * 1000000 + hh * 10000 + mm * 100 + ss).

## formatDateTime {#formatdatetime}

Функция преобразует дату-и-время в строку по заданному шаблону. Важно: шаблон — константное выражение, поэтому использовать разные шаблоны в одной колонке не получится.

**Синтаксис**

``` sql
formatDateTime(Time, Format\[, Timezone\])
```

**Возвращаемое значение**

Возвращает значение времени и даты в определенном вами формате.

**Поля подстановки**

Используйте поля подстановки для того, чтобы определить шаблон для выводимой строки. В колонке «Пример» результат работы функции для времени `2018-01-02 22:33:44`.

| Поле        | Описание                                                             | Пример     |
|-------------|----------------------------------------------------------------------|------------|
| %C          | номер года, поделённый на 100 (00-99)                                | 20         |
| %d          | день месяца, с ведущим нулём (01-31)                                 | 02         |
| %D          | короткая запись %m/%d/%y                                             | 01/02/18   |
| %e          | день месяца, с ведущим пробелом ( 1-31)                              | &nbsp; 2   |
| %F          | короткая запись %Y-%m-%d                                             | 2018-01-02 |
| %G          | четырехзначный формат вывода ISO-года, который основывается на особом подсчете номера недели согласно [стандарту ISO 8601](https://ru.wikipedia.org/wiki/ISO_8601), обычно используется вместе с %V   | 2018       |
| %g          | двузначный формат вывода года по стандарту ISO 8601                  | 18         |
| %H          | час в 24-часовом формате (00-23)                                     | 22         |
| %I          | час в 12-часовом формате (01-12)                                     | 10         |
| %j          | номер дня в году, с ведущими нулями (001-366)                        | 002        |
| %m          | месяц, с ведущим нулём (01-12)                                       | 01         |
| %M          | минуты, с ведущим нулём (00-59)                                      | 33         |
| %n          | символ переноса строки (‘’)                                          |            |
| %p          | обозначения AM или PM                                                | PM         |
| %Q          | квартал (1-4)                                                        | 1          |
| %R          | короткая запись %H:%M                                                | 22:33      |
| %S          | секунды, с ведущими нулями (00-59)                                   | 44         |
| %t          | символ табуляции (’)                                                 |            |
| %T          | формат времени ISO 8601, одинаковый с %H:%M:%S                       | 22:33:44   |
| %u          | номер дня недели согласно ISO 8601, понедельник - 1, воскресенье - 7 | 2          |
| %V          | номер недели согласно ISO 8601 (01-53)                               | 01         |
| %w          | номер дня недели, начиная с воскресенья (0-6)                        | 2          |
| %y          | год, последние 2 цифры (00-99)                                       | 18         |
| %Y          | год, 4 цифры                                                         | 2018       |
| %%          | символ %                                                             | %          |

**Пример**

Запрос:

``` sql
SELECT formatDateTime(toDate('2010-01-04'), '%g');
```

Результат:

```
┌─formatDateTime(toDate('2010-01-04'), '%g')─┐
│ 10                                         │
└────────────────────────────────────────────┘
```

## dateName {#dataname}

Возвращает указанную часть даты.

**Синтаксис**

``` sql
dateName(date_part, date)
```

**Аргументы**

-   `date_part` — часть даты. Возможные значения: 'year', 'quarter', 'month', 'week', 'dayofyear', 'day', 'weekday', 'hour', 'minute', 'second'. [String](../../sql-reference/data-types/string.md).
-   `date` — дата. [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md) или [DateTime64](../../sql-reference/data-types/datetime64.md).
-   `timezone` — часовой пояс. Необязательный аргумент. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   Указанная часть даты.

Тип: [String](../../sql-reference/data-types/string.md#string).

**Пример**

Запрос:

```sql
WITH toDateTime('2021-04-14 11:22:33') AS date_value
SELECT dateName('year', date_value), dateName('month', date_value), dateName('day', date_value);
```

Результат:

```text
┌─dateName('year', date_value)─┬─dateName('month', date_value)─┬─dateName('day', date_value)─┐
│ 2021                         │ April                         │ 14                          │
└──────────────────────────────┴───────────────────────────────┴─────────────────────────────
```

## FROM\_UNIXTIME {#fromunixtime}

Функция преобразует Unix timestamp в календарную дату и время.

**Примеры**

Если указан только один аргумент типа [Integer](../../sql-reference/data-types/int-uint.md), то функция действует так же, как [toDateTime](../../sql-reference/functions/type-conversion-functions.md#todatetime), и возвращает тип [DateTime](../../sql-reference/data-types/datetime.md).

Запрос:

```sql
SELECT FROM_UNIXTIME(423543535);
```

Результат:

```text
┌─FROM_UNIXTIME(423543535)─┐
│      1983-06-04 10:58:55 │
└──────────────────────────┘
```

В случае, когда есть два аргумента: первый типа [Integer](../../sql-reference/data-types/int-uint.md) или [DateTime](../../sql-reference/data-types/datetime.md), а второй является строкой постоянного формата — функция работает также, как [formatDateTime](#formatdatetime), и возвращает значение типа [String](../../sql-reference/data-types/string.md#string).


Запрос:

```sql
SELECT FROM_UNIXTIME(1234334543, '%Y-%m-%d %R:%S') AS DateTime;
```

Результат:

```text
┌─DateTime────────────┐
│ 2009-02-11 14:42:23 │
└─────────────────────┘
```
