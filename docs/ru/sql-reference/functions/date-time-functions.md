---
toc_priority: 39
toc_title: "\u0424\u0443\u043d\u043a\u0446\u0438\u0438\u0020\u0434\u043b\u044f\u0020\u0440\u0430\u0431\u043e\u0442\u044b\u0020\u0441\u0020\u0434\u0430\u0442\u0430\u043c\u0438\u0020\u0438\u0020\u0432\u0440\u0435\u043c\u0435\u043d\u0435\u043c"
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

Поддерживаются только часовые пояса, отличающиеся от UTC на целое число часов.

## toTimeZone {#totimezone}

Переводит дату или дату-с-временем в указанный часовой пояс. Часовой пояс (таймзона) это атрибут типов Date/DateTime, внутреннее значение (количество секунд) поля таблицы или колонки результата не изменяется, изменяется тип поля и автоматически его текстовое отображение.

```sql
SELECT
    toDateTime('2019-01-01 00:00:00', 'UTC') AS time_utc,
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

## toYear {#toyear}

Переводит дату или дату-с-временем в число типа UInt16, содержащее номер года (AD).

## toMonth {#tomonth}

Переводит дату или дату-с-временем в число типа UInt8, содержащее номер месяца (1-12).

## toDayOfMonth {#todayofmonth}

Переводит дату или дату-с-временем в число типа UInt8, содержащее номер дня в месяце (1-31).

## toDayOfWeek {#todayofweek}

Переводит дату или дату-с-временем в число типа UInt8, содержащее номер дня в неделе (понедельник - 1, воскресенье - 7).

## toHour {#tohour}

Переводит дату-с-временем в число типа UInt8, содержащее номер часа в сутках (0-23).
Функция исходит из допущения, что перевод стрелок вперёд, если осуществляется, то на час, в два часа ночи, а перевод стрелок назад, если осуществляется, то на час, в три часа ночи (что, в общем, не верно - даже в Москве два раза перевод стрелок был осуществлён в другое время).

## toMinute {#tominute}

Переводит дату-с-временем в число типа UInt8, содержащее номер минуты в часе (0-59).

## toSecond {#tosecond}

Переводит дату-с-временем в число типа UInt8, содержащее номер секунды в минуте (0-59).
Секунды координации не учитываются.

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
SELECT toUnixTimestamp('2017-11-05 08:07:47', 'Asia/Tokyo') AS unix_timestamp
```

Результат:

``` text
┌─unix_timestamp─┐
│     1509836867 │
└────────────────┘
```

## toStartOfYear {#tostartofyear}

Округляет дату или дату-с-временем вниз до первого дня года.
Возвращается дата.

## toStartOfQuarter {#tostartofquarter}

Округляет дату или дату-с-временем вниз до первого дня квартала.
Первый день квартала - это одно из 1 января, 1 апреля, 1 июля, 1 октября.
Возвращается дата.

## toStartOfMonth {#tostartofmonth}

Округляет дату или дату-с-временем вниз до первого дня месяца.
Возвращается дата.

!!! attention "Attention"
    Возвращаемое значение для некорректных дат зависит от реализации. ClickHouse может вернуть нулевую дату, выбросить исключение, или выполнить «естественное» перетекание дат между месяцами.

## toMonday {#tomonday}

Округляет дату или дату-с-временем вниз до ближайшего понедельника.
Возвращается дата.

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
toStartOfSecond(value[, timezone])
```

**Параметры**

-   `value` — Дата и время. [DateTime64](../data-types/datetime64.md).
-   `timezone` — [Часовой пояс](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) для возвращаемого значения (необязательно). Если параметр не задан, используется часовой пояс параметра `value`. [String](../data-types/string.md). 

**Возвращаемое значение**

- Входное значение с отсеченными долями секунды.

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

**См. также**

- Часовая зона сервера, конфигурационный параметр [timezone](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone).


## toStartOfFiveMinute {#tostartoffiveminute}

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

## date_trunc {#date_trunc}

Отсекает от даты и времени части, меньшие чем указанная часть.

**Синтаксис** 

``` sql
date_trunc(unit, value[, timezone])
```

Синоним: `dateTrunc`. 

**Параметры**

-   `unit` — Название части даты или времени. [String](../syntax.md#syntax-string-literal).
    Возможные значения:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

-   `value` — Дата и время. [DateTime](../../sql-reference/data-types/datetime.md) или [DateTime64](../../sql-reference/data-types/datetime64.md).
-   `timezone` — [Часовой пояс](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) для возвращаемого значения (необязательно). Если параметр не задан, используется часовой пояс параметра `value`. [String](../../sql-reference/data-types/string.md)

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

**См. также**

-   [toStartOfInterval](#tostartofintervaltime-or-data-interval-x-unit-time-zone)

## now {#now}

Возвращает текущую дату и время. 

**Синтаксис** 

``` sql
now([timezone])
```

**Параметры**

-   `timezone` — [часовой пояс](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) для возвращаемого значения (необязательно). [String](../../sql-reference/data-types/string.md)

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

Принимает ноль аргументов и возвращает текущую дату на один из моментов выполнения запроса.
То же самое, что toDate(now())

## yesterday {#yesterday}

Принимает ноль аргументов и возвращает вчерашнюю дату на один из моментов выполнения запроса.
Делает то же самое, что today() - 1.

## dateDiff {#datediff}

Вычисляет разницу между двумя значениями дат с временем.

**Синтаксис**

``` sql
dateDiff('unit', startdate, enddate, [timezone])
```

**Параметры**

-   `unit` — Единица измерения времени, в которой будет вычислена разница между `startdate` и `enddate`. [String](../syntax.md#syntax-string-literal).

        Поддерживаемые значения:

        | unit   |
        | ------ |
        |second  |
        |minute  |
        |hour    |
        |day     |
        |week    |
        |month   |
        |quarter |
        |year    |

-   `startdate` — Первая дата. [Date](../../sql-reference/functions/date-time-functions.md) или [DateTime](../../sql-reference/functions/date-time-functions.md).

-   `enddate` — Вторая дата. [Date](../../sql-reference/functions/date-time-functions.md) или [DateTime](../../sql-reference/functions/date-time-functions.md).

-   `timezone` — Опциональный параметр. Если определен, применяется к обоим значениям: `startdate` и `enddate`. Если не определен, используются часовые пояса `startdate` и `enddate`. Если часовые пояса не совпадают, вернется неожидаемый результат.

**Возвращаемое значение**

Разница между `startdate` и `enddate`, выраженная в `unit`.

Тип: `int`.

**Пример**

Запрос:

``` sql
SELECT dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'));
```

Ответ:

``` text
┌─dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'))─┐
│                                                                                     25 │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

## timeSlot {#timeslot}

Округляет время до получаса.
Эта функция является специфичной для Яндекс.Метрики, так как пол часа - минимальное время, для которого, если соседние по времени хиты одного посетителя на одном счётчике отстоят друг от друга строго более, чем на это время, визит может быть разбит на два визита. То есть, кортежи (номер счётчика, идентификатор посетителя, тайм-слот) могут использоваться для поиска хитов, входящий в соответствующий визит.

## timeSlots(StartTime, Duration,\[, Size\]) {#timeslotsstarttime-duration-size}

Для интервала времени, начинающегося в ‘StartTime’ и продолжающегося ‘Duration’ секунд, возвращает массив моментов времени, состоящий из округлений вниз до ‘Size’ точек в секундах из этого интервала. ‘Size’ - необязательный параметр, константный UInt32, по умолчанию равен 1800.

Например, `timeSlots(toDateTime('2012-01-01 12:20:00'), toUInt32(600)) = [toDateTime('2012-01-01 12:00:00'), toDateTime('2012-01-01 12:30:00')]`.
Это нужно для поиска хитов, входящих в соответствующий визит.

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
| %e          | день месяца, с ведущим пробелом ( 1-31)                              | 2          |
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
SELECT formatDateTime(toDate('2010-01-04'), '%g')
```

Ответ:

```
┌─formatDateTime(toDate('2010-01-04'), '%g')─┐
│ 10                                         │
└────────────────────────────────────────────┘
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/functions/date_time_functions/) <!--hide-->
