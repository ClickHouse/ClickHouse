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

For DateTime argument: converts value to its internal numeric representation (Unix Timestamp).
For String argument: parse datetime from string according to the timezone (optional second argument, server timezone is used by default) and returns the corresponding unix timestamp.
For Date argument: the behaviour is unspecified.

**Syntax**

``` sql
toUnixTimestamp(datetime)
toUnixTimestamp(str, [timezone])
```

**Returned value**

-   Returns the unix timestamp.

Type: `UInt32`.

**Example**

Query:

``` sql
SELECT toUnixTimestamp('2017-11-05 08:07:47', 'Asia/Tokyo') AS unix_timestamp
```

Result:

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

## toStartOfInterval(time\_or\_data, INTERVAL x unit \[, time\_zone\]) {#tostartofintervaltime-or-data-interval-x-unit-time-zone}

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

## now {#now}

Принимает ноль аргументов и возвращает текущее время на один из моментов выполнения запроса.
Функция возвращает константу, даже если запрос выполнялся долго.

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

## formatDateTime(Time, Format\[, Timezone\]) {#formatdatetime}

Функция преобразования даты-с-временем в String согласно заданному шаблону. Важно - шаблон является константным выражением, т.е. невозможно использование разных шаблонов в одной колонке.

Поддерживаемые модификаторы в шаблоне Format:
(колонка «Пример» показана для времени `2018-01-02 22:33:44`)

| Модификатор | Описание                                                             | Пример     |
|-------------|----------------------------------------------------------------------|------------|
| %C          | номер года, поделённый на 100 (00-99)                                | 20         |
| %d          | день месяца, с ведущим нулём (01-31)                                 | 02         |
| %D          | короткая запись %m/%d/%y                                             | 01/02/18   |
| %e          | день месяца, с ведущим пробелом ( 1-31)                              | 2          |
| %F          | короткая запись %Y-%m-%d                                             | 2018-01-02 |
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

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/functions/date_time_functions/) <!--hide-->
