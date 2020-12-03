---
toc_priority: 38
toc_title: "\u0424\u0443\u043d\u043a\u0446\u0438\u0438\u0020\u043f\u0440\u0435\u043e\u0431\u0440\u0430\u0437\u043e\u0432\u0430\u043d\u0438\u044f\u0020\u0442\u0438\u043f\u043e\u0432"
---

# Функции преобразования типов {#funktsii-preobrazovaniia-tipov}

## Общие проблемы преобразования чисел {#numeric-conversion-issues}

При преобразовании значения из одного типа в другой необходимо помнить, что в общем случае это небезопасная операция, которая может привести к потере данных. Потеря данных может произойти при попытке сконвертировать тип данных значения от большего к меньшему или при конвертировании между различными классами типов данных.

Поведение ClickHouse при конвертировании похоже на [поведение C++ программ](https://en.cppreference.com/w/cpp/language/implicit_conversion).

## toInt(8\|16\|32\|64\|128\|256) {#toint8163264}

Преобразует входное значение к типу [Int](../../sql-reference/functions/type-conversion-functions.md). Семейство функций включает:

-   `toInt8(expr)` — возвращает значение типа `Int8`.
-   `toInt16(expr)` — возвращает значение типа `Int16`.
-   `toInt32(expr)` — возвращает значение типа `Int32`.
-   `toInt64(expr)` — возвращает значение типа `Int64`.
-   `toInt128(expr)` — возвращает значение типа `Int128`.
-   `toInt256(expr)` — возвращает значение типа `Int256`.

**Параметры**

-   `expr` — [выражение](../syntax.md#syntax-expressions) возвращающее число или строку с десятичным представление числа. Бинарное, восьмеричное и шестнадцатеричное представление числа не поддержаны. Ведущие нули обрезаются.

**Возвращаемое значение**

Целое число типа `Int8`, `Int16`, `Int32`, `Int64`, `Int128` или `Int256`.

Функции используют [округление к нулю](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), т.е. обрезают дробную часть числа.

Поведение функций для аргументов [NaN и Inf](../../sql-reference/functions/type-conversion-functions.md#data_type-float-nan-inf) не определено. При использовании функций помните о возможных проблемах при [преобразовании чисел](#numeric-conversion-issues).

**Пример**

``` sql
SELECT toInt64(nan), toInt32(32), toInt16('16'), toInt8(8.8)
```

``` text
┌─────────toInt64(nan)─┬─toInt32(32)─┬─toInt16('16')─┬─toInt8(8.8)─┐
│ -9223372036854775808 │          32 │            16 │           8 │
└──────────────────────┴─────────────┴───────────────┴─────────────┘
```

## toInt(8\|16\|32\|64\|128\|256)OrZero {#toint8163264orzero}

Принимает аргумент типа String и пытается его распарсить в Int(8\|16\|32\|64\|128\|256). Если не удалось - возвращает 0.

**Пример**

``` sql
select toInt64OrZero('123123'), toInt8OrZero('123qwe123')
```

``` text
┌─toInt64OrZero('123123')─┬─toInt8OrZero('123qwe123')─┐
│                  123123 │                         0 │
└─────────────────────────┴───────────────────────────┘
```

## toInt(8\|16\|32\|64\|128\|256)OrNull {#toint8163264ornull}

Принимает аргумент типа String и пытается его распарсить в Int(8\|16\|32\|64\|128\|256). Если не удалось - возвращает NULL.

**Пример**

``` sql
select toInt64OrNull('123123'), toInt8OrNull('123qwe123')
```

``` text
┌─toInt64OrNull('123123')─┬─toInt8OrNull('123qwe123')─┐
│                  123123 │                      ᴺᵁᴸᴸ │
└─────────────────────────┴───────────────────────────┘
```

## toUInt(8\|16\|32\|64\|256) {#touint8163264}

Преобраует входное значение к типу [UInt](../../sql-reference/functions/type-conversion-functions.md). Семейство функций включает:

-   `toUInt8(expr)` — возвращает значение типа `UInt8`.
-   `toUInt16(expr)` — возвращает значение типа `UInt16`.
-   `toUInt32(expr)` — возвращает значение типа `UInt32`.
-   `toUInt64(expr)` — возвращает значение типа `UInt64`.
-   `toUInt256(expr)` — возвращает значение типа `UInt256`.

**Параметры**

-   `expr` — [выражение](../syntax.md#syntax-expressions) возвращающее число или строку с десятичным представление числа. Бинарное, восьмеричное и шестнадцатеричное представление числа не поддержаны. Ведущие нули обрезаются.

**Возвращаемое значение**

Целое число типа `UInt8`, `UInt16`, `UInt32`, `UInt64` или `UInt256`.

Функции используют [округление к нулю](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), т.е. обрезают дробную часть числа.

Поведение функций для аргументов [NaN и Inf](../../sql-reference/functions/type-conversion-functions.md#data_type-float-nan-inf) не определено. Если передать строку, содержащую отрицательное число, например `'-32'`, ClickHouse генерирует исключение. При использовании функций помните о возможных проблемах при [преобразовании чисел](#numeric-conversion-issues).

**Пример**

``` sql
SELECT toUInt64(nan), toUInt32(-32), toUInt16('16'), toUInt8(8.8)
```

``` text
┌───────toUInt64(nan)─┬─toUInt32(-32)─┬─toUInt16('16')─┬─toUInt8(8.8)─┐
│ 9223372036854775808 │    4294967264 │             16 │            8 │
└─────────────────────┴───────────────┴────────────────┴──────────────┘
```

## toUInt(8\|16\|32\|64\|256)OrZero {#touint8163264orzero}

## toUInt(8\|16\|32\|64\|256)OrNull {#touint8163264ornull}

## toFloat(32\|64) {#tofloat3264}

## toFloat(32\|64)OrZero {#tofloat3264orzero}

## toFloat(32\|64)OrNull {#tofloat3264ornull}

## toDate {#todate}

## toDateOrZero {#todateorzero}

## toDateOrNull {#todateornull}

## toDateTime {#todatetime}

## toDateTimeOrZero {#todatetimeorzero}

## toDateTimeOrNull {#todatetimeornull}

## toDecimal(32\|64\|128\|256) {#todecimal3264128}

Преобразует `value` к типу данных [Decimal](../../sql-reference/functions/type-conversion-functions.md) с точностью `S`. `value` может быть числом или строкой. Параметр `S` (scale) задаёт число десятичных знаков.

-   `toDecimal32(value, S)`
-   `toDecimal64(value, S)`
-   `toDecimal128(value, S)`
-   `toDecimal256(value, S)`

## toDecimal(32\|64\|128\|256)OrNull {#todecimal3264128ornull}

Преобразует входную строку в значение с типом данных [Nullable (Decimal (P, S))](../../sql-reference/functions/type-conversion-functions.md). Семейство функций включает в себя:

-   `toDecimal32OrNull(expr, S)` — Возвращает значение типа `Nullable(Decimal32(S))`.
-   `toDecimal64OrNull(expr, S)` — Возвращает значение типа `Nullable(Decimal64(S))`.
-   `toDecimal128OrNull(expr, S)` — Возвращает значение типа `Nullable(Decimal128(S))`.
-   `toDecimal256OrNull(expr, S)` — Возвращает значение типа `Nullable(Decimal256(S))`.

Эти функции следует использовать вместо функций `toDecimal*()`, если при ошибке обработки входного значения вы хотите получать `NULL` вместо исключения.

**Параметры**

-   `expr` — [выражение](../syntax.md#syntax-expressions), возвращающее значение типа [String](../../sql-reference/functions/type-conversion-functions.md). ClickHouse ожидает текстовое представление десятичного числа. Например, `'1.111'`.
-   `S` — количество десятичных знаков в результирующем значении.

**Возвращаемое значение**

Значение типа `Nullable(Decimal(P,S))`. Значение содержит:

-   Число с `S` десятичными знаками, если ClickHouse распознал число во входной строке.
-   `NULL`, если ClickHouse не смог распознать число во входной строке или входное число содержит больше чем `S` десятичных знаков.

**Примеры**

``` sql
SELECT toDecimal32OrNull(toString(-1.111), 5) AS val, toTypeName(val)
```

``` text
┌──────val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 5))─┐
│ -1.11100 │ Nullable(Decimal(9, 5))                            │
└──────────┴────────────────────────────────────────────────────┘
```

``` sql
SELECT toDecimal32OrNull(toString(-1.111), 2) AS val, toTypeName(val)
```

``` text
┌──val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 2))─┐
│ ᴺᵁᴸᴸ │ Nullable(Decimal(9, 2))                            │
└──────┴────────────────────────────────────────────────────┘
```

## toDecimal(32\|64\|128\|256)OrZero {#todecimal3264128orzero}

Преобразует тип входного значения в [Decimal (P, S)](../../sql-reference/functions/type-conversion-functions.md). Семейство функций включает в себя:

-   `toDecimal32OrZero( expr, S)` — возвращает значение типа `Decimal32(S)`.
-   `toDecimal64OrZero( expr, S)` — возвращает значение типа `Decimal64(S)`.
-   `toDecimal128OrZero( expr, S)` — возвращает значение типа `Decimal128(S)`.
-   `toDecimal256OrZero( expr, S)` — возвращает значение типа `Decimal256(S)`.

Эти функции следует использовать вместо функций `toDecimal*()`, если при ошибке обработки входного значения вы хотите получать `0` вместо исключения.

**Параметры**

-   `expr` — [выражение](../syntax.md#syntax-expressions), возвращающее значение типа [String](../../sql-reference/functions/type-conversion-functions.md). ClickHouse ожидает текстовое представление десятичного числа. Например, `'1.111'`.
-   `S` — количество десятичных знаков в результирующем значении.

**Возвращаемое значение**

Значение типа `Nullable(Decimal(P,S))`. `P` равно числовой части имени функции. Например, для функции `toDecimal32OrZero`, `P = 32`. Значение содержит:

-   Число с `S` десятичными знаками, если ClickHouse распознал число во входной строке.
-   0 c `S` десятичными знаками, если ClickHouse не смог распознать число во входной строке или входное число содержит больше чем `S` десятичных знаков.

**Пример**

``` sql
SELECT toDecimal32OrZero(toString(-1.111), 5) AS val, toTypeName(val)
```

``` text
┌──────val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 5))─┐
│ -1.11100 │ Decimal(9, 5)                                      │
└──────────┴────────────────────────────────────────────────────┘
```

``` sql
SELECT toDecimal32OrZero(toString(-1.111), 2) AS val, toTypeName(val)
```

``` text
┌──val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 2))─┐
│ 0.00 │ Decimal(9, 2)                                      │
└──────┴────────────────────────────────────────────────────┘
```

## toString {#tostring}

Функции преобразования между числами, строками (но не фиксированными строками), датами и датами-с-временем.
Все эти функции принимают один аргумент.

При преобразовании в строку или из строки, производится форматирование или парсинг значения по тем же правилам, что и для формата TabSeparated (и почти всех остальных текстовых форматов). Если распарсить строку не удаётся - кидается исключение и выполнение запроса прерывается.

При преобразовании даты в число или наоборот, дате соответствует число дней от начала unix эпохи.
При преобразовании даты-с-временем в число или наоборот, дате-с-временем соответствует число секунд от начала unix эпохи.

Форматы даты и даты-с-временем для функций toDate/toDateTime определены следующим образом:

``` text
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

В качестве исключения, если делается преобразование из числа типа UInt32, Int32, UInt64, Int64 в Date, и если число больше или равно 65536, то число рассматривается как unix timestamp (а не как число дней) и округляется до даты. Это позволяет поддержать распространённый случай, когда пишут toDate(unix_timestamp), что иначе было бы ошибкой и требовало бы написания более громоздкого toDate(toDateTime(unix_timestamp))

Преобразование между датой и датой-с-временем производится естественным образом: добавлением нулевого времени или отбрасыванием времени.

Преобразование между числовыми типами производится по тем же правилам, что и присваивание между разными числовыми типами в C++.

Дополнительно, функция toString от аргумента типа DateTime может принимать второй аргумент String - имя тайм-зоны. Пример: `Asia/Yekaterinburg` В этом случае, форматирование времени производится согласно указанной тайм-зоне.

``` sql
SELECT
    now() AS now_local,
    toString(now(), 'Asia/Yekaterinburg') AS now_yekat
```

``` text
┌───────────now_local─┬─now_yekat───────────┐
│ 2016-06-15 00:11:21 │ 2016-06-15 02:11:21 │
└─────────────────────┴─────────────────────┘
```

Также смотрите функцию `toUnixTimestamp`.

## toFixedString(s, N) {#tofixedstrings-n}

Преобразует аргумент типа String в тип FixedString(N) (строку фиксированной длины N). N должно быть константой.
Если строка имеет меньше байт, чем N, то она дополняется нулевыми байтами справа. Если строка имеет больше байт, чем N - кидается исключение.

## toStringCutToZero(s) {#tostringcuttozeros}

Принимает аргумент типа String или FixedString. Возвращает String, вырезая содержимое строки до первого найденного нулевого байта.

Пример:

``` sql
SELECT toFixedString('foo', 8) AS s, toStringCutToZero(s) AS s_cut
```

``` text
┌─s─────────────┬─s_cut─┐
│ foo\0\0\0\0\0 │ foo   │
└───────────────┴───────┘
```

``` sql
SELECT toFixedString('foo\0bar', 8) AS s, toStringCutToZero(s) AS s_cut
```

``` text
┌─s──────────┬─s_cut─┐
│ foo\0bar\0 │ foo   │
└────────────┴───────┘
```

## reinterpretAsUInt(8\|16\|32\|64) {#reinterpretasuint8163264}

## reinterpretAsInt(8\|16\|32\|64) {#reinterpretasint8163264}

## reinterpretAsFloat(32\|64) {#reinterpretasfloat3264}

## reinterpretAsDate {#reinterpretasdate}

## reinterpretAsDateTime {#reinterpretasdatetime}

Функции принимают строку и интерпретируют байты, расположенные в начале строки, как число в host order (little endian). Если строка имеет недостаточную длину, то функции работают так, как будто строка дополнена необходимым количеством нулевых байт. Если строка длиннее, чем нужно, то лишние байты игнорируются. Дата интерпретируется, как число дней с начала unix-эпохи, а дата-с-временем - как число секунд с начала unix-эпохи.

## reinterpretAsString {#type_conversion_functions-reinterpretAsString}

Функция принимает число или дату или дату-с-временем и возвращает строку, содержащую байты, представляющие соответствующее значение в host order (little endian). При этом, отбрасываются нулевые байты с конца. Например, значение 255 типа UInt32 будет строкой длины 1 байт.

## reinterpretAsUUID {#reinterpretasuuid}

Функция принимает шестнадцатибайтную строку и интерпретирует ее байты в network order (big-endian). Если строка имеет недостаточную длину, то функция работает так, как будто строка дополнена необходимым количетсвом нулевых байт с конца. Если строка длиннее, чем шестнадцать байт, то игнорируются лишние байты с конца.

**Синтаксис**

``` sql
reinterpretAsUUID(fixed_string)
```

**Параметры**

-   `fixed_string` — cтрока с big-endian порядком байтов. [FixedString](../../sql-reference/data-types/fixedstring.md#fixedstring).

**Возвращаемое значение**

-   Значение типа [UUID](../../sql-reference/data-types/uuid.md#uuid-data-type).

**Примеры**

Интерпретация строки как UUID.

Запрос:

``` sql
SELECT reinterpretAsUUID(reverse(unhex('000102030405060708090a0b0c0d0e0f')))
```

Результат:

``` text
┌─reinterpretAsUUID(reverse(unhex('000102030405060708090a0b0c0d0e0f')))─┐
│                                  08090a0b-0c0d-0e0f-0001-020304050607 │
└───────────────────────────────────────────────────────────────────────┘
```

Переход в UUID и обратно.

Запрос:

``` sql
WITH
    generateUUIDv4() AS uuid,
    identity(lower(hex(reverse(reinterpretAsString(uuid))))) AS str,
    reinterpretAsUUID(reverse(unhex(str))) AS uuid2
SELECT uuid = uuid2;
```

Результат:

``` text
┌─equals(uuid, uuid2)─┐
│                   1 │
└─────────────────────┘
```

## CAST(x, T) {#type_conversion_function-cast}

Преобразует x в тип данных t.
Поддерживается также синтаксис CAST(x AS t).

Пример:

``` sql
SELECT
    '2016-06-15 23:00:00' AS timestamp,
    CAST(timestamp AS DateTime) AS datetime,
    CAST(timestamp AS Date) AS date,
    CAST(timestamp, 'String') AS string,
    CAST(timestamp, 'FixedString(22)') AS fixed_string
```

``` text
┌─timestamp───────────┬────────────datetime─┬───────date─┬─string──────────────┬─fixed_string──────────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00\0\0\0 │
└─────────────────────┴─────────────────────┴────────────┴─────────────────────┴───────────────────────────┘
```

Преобразование в FixedString(N) работает только для аргументов типа String или FixedString(N).

Поддержано преобразование к типу [Nullable](../../sql-reference/functions/type-conversion-functions.md) и обратно. Пример:

``` sql
SELECT toTypeName(x) FROM t_null
```

``` text
┌─toTypeName(x)─┐
│ Int8          │
│ Int8          │
└───────────────┘
```

``` sql
SELECT toTypeName(CAST(x, 'Nullable(UInt16)')) FROM t_null
```

``` text
┌─toTypeName(CAST(x, 'Nullable(UInt16)'))─┐
│ Nullable(UInt16)                        │
│ Nullable(UInt16)                        │
└─────────────────────────────────────────┘
```

**См. также**

-   Настройка [cast_keep_nullable](../../operations/settings/settings.md#cast_keep_nullable)

## toInterval(Year\|Quarter\|Month\|Week\|Day\|Hour\|Minute\|Second) {#function-tointerval}

Приводит аргумент из числового типа данных к типу данных [IntervalType](../../sql-reference/data-types/special-data-types/interval.md).

**Синтаксис**

``` sql
toIntervalSecond(number)
toIntervalMinute(number)
toIntervalHour(number)
toIntervalDay(number)
toIntervalWeek(number)
toIntervalMonth(number)
toIntervalQuarter(number)
toIntervalYear(number)
```

**Параметры**

-   `number` — длительность интервала. Положительное целое число.

**Возвращаемые значения**

-   Значение с типом данных `Interval`.

**Пример**

``` sql
WITH
    toDate('2019-01-01') AS date,
    INTERVAL 1 WEEK AS interval_week,
    toIntervalWeek(1) AS interval_to_week
SELECT
    date + interval_week,
    date + interval_to_week
```

``` text
┌─plus(date, interval_week)─┬─plus(date, interval_to_week)─┐
│                2019-01-08 │                   2019-01-08 │
└───────────────────────────┴──────────────────────────────┘
```

## parseDateTimeBestEffort {#parsedatetimebesteffort}

Преобразует дату и время в [строковом](../../sql-reference/functions/type-conversion-functions.md) представлении к типу данных [DateTime](../../sql-reference/functions/type-conversion-functions.md#data_type-datetime).

Функция распознаёт форматы [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), [RFC 1123 - 5.2.14 RFC-822 Date and Time Specification](https://tools.ietf.org/html/rfc1123#page-55), формат даты времени ClickHouse’s а также некоторые другие форматы.

**Синтаксис**

``` sql
parseDateTimeBestEffort(time_string[, time_zone]);
```

**Параметры**

-   `time_string` — строка, содержащая дату и время для преобразования. [String](../../sql-reference/functions/type-conversion-functions.md).
-   `time_zone` — часовой пояс. Функция анализирует `time_string` в соответствии с заданным часовым поясом. [String](../../sql-reference/functions/type-conversion-functions.md).

**Поддерживаемые нестандартные форматы**

-   [Unix timestamp](https://ru.wikipedia.org/wiki/Unix-время) в строковом представлении. 9 или 10 символов.
-   Строка с датой и временем: `YYYYMMDDhhmmss`, `DD/MM/YYYY hh:mm:ss`, `DD-MM-YY hh:mm`, `YYYY-MM-DD hh:mm:ss`, etc.
-   Строка с датой, но без времени: `YYYY`, `YYYYMM`, `YYYY*MM`, `DD/MM/YYYY`, `DD-MM-YY` и т.д.
-   Строка с временем, и с днём: `DD`, `DD hh`, `DD hh:mm`. В этом случае `YYYY-MM` принимается равным `2000-01`.
-   Строка, содержащая дату и время вместе с информацией о часовом поясе: `YYYY-MM-DD hh:mm:ss ±h:mm`, и т.д. Например, `2020-12-12 17:36:00 -5:00`.

Для всех форматов с разделителями функция распознаёт названия месяцев, выраженных в виде полного англоязычного имени месяца или в виде первых трёх символов имени месяца. Примеры: `24/DEC/18`, `24-Dec-18`, `01-September-2018`.

**Возвращаемое значение**

-   `time_string` преобразованная к типу данных `DateTime`.

**Примеры**

Запрос:

``` sql
SELECT parseDateTimeBestEffort('12/12/2020 12:12:57')
AS parseDateTimeBestEffort;
```

Результат:

``` text
┌─parseDateTimeBestEffort─┐
│     2020-12-12 12:12:57 │
└─────────────────────────┘
```

Запрос:

``` sql
SELECT parseDateTimeBestEffort('Sat, 18 Aug 2018 07:22:16 GMT', 'Europe/Moscow')
AS parseDateTimeBestEffort
```

Результат:

``` text
┌─parseDateTimeBestEffort─┐
│     2018-08-18 10:22:16 │
└─────────────────────────┘
```

Запрос:

``` sql
SELECT parseDateTimeBestEffort('1284101485')
AS parseDateTimeBestEffort
```

Результат:

``` text
┌─parseDateTimeBestEffort─┐
│     2015-07-07 12:04:41 │
└─────────────────────────┘
```

Запрос:

``` sql
SELECT parseDateTimeBestEffort('2018-12-12 10:12:12')
AS parseDateTimeBestEffort
```

Результат:

``` text
┌─parseDateTimeBestEffort─┐
│     2018-12-12 10:12:12 │
└─────────────────────────┘
```

Запрос:

``` sql
SELECT parseDateTimeBestEffort('10 20:19')
```

Результат:

``` text
┌─parseDateTimeBestEffort('10 20:19')─┐
│                 2000-01-10 20:19:00 │
└─────────────────────────────────────┘
```

**См. также**

-   [Информация о формате ISO 8601 от @xkcd](https://xkcd.com/1179/)
-   [RFC 1123](https://tools.ietf.org/html/rfc1123)
-   [toDate](#todate)
-   [toDateTime](#todatetime)

## parseDateTimeBestEffortUS {#parsedatetimebesteffortUS}

Эта функция похожа на [‘parseDateTimeBestEffort’](#parsedatetimebesteffort), но разница состоит в том, что в она предполагает американский формат даты (`MM/DD/YYYY` etc.) в случае неоднозначности.

**Синтаксис**

``` sql
parseDateTimeBestEffortUS(time_string [, time_zone]);
```

**Параметры**

-   `time_string` — строка, содержащая дату и время для преобразования. [String](../../sql-reference/data-types/string.md).
-   `time_zone` — часовой пояс. Функция анализирует `time_string` в соответствии с часовым поясом. [String](../../sql-reference/data-types/string.md).

**Поддерживаемые нестандартные форматы**

-   Строка, содержащая 9-10 цифр [unix timestamp](https://en.wikipedia.org/wiki/Unix_time).
-   Строка, содержащая дату и время: `YYYYMMDDhhmmss`, `MM/DD/YYYY hh:mm:ss`, `MM-DD-YY hh:mm`, `YYYY-MM-DD hh:mm:ss`, etc.
-   Строка с датой, но без времени: `YYYY`, `YYYYMM`, `YYYY*MM`, `MM/DD/YYYY`, `MM-DD-YY` etc.
-   Строка, содержащая день и время: `DD`, `DD hh`, `DD hh:mm`. В этом случае `YYYY-MM` заменяется на `2000-01`.
-   Строка, содержащая дату и время, а также информацию о часовом поясе: `YYYY-MM-DD hh:mm:ss ±h:mm` и т.д. Например, `2020-12-12 17:36:00 -5:00`.

**Возвращаемое значение**

-   `time_string` преобразован в тип данных `DateTime`.

**Примеры**

Запрос:

``` sql
SELECT parseDateTimeBestEffortUS('09/12/2020 12:12:57')
AS parseDateTimeBestEffortUS;
```

Ответ:

``` text
┌─parseDateTimeBestEffortUS─┐
│     2020-09-12 12:12:57   │
└─────────────────────────——┘
```

Запрос:

``` sql
SELECT parseDateTimeBestEffortUS('09-12-2020 12:12:57')
AS parseDateTimeBestEffortUS;
```

Ответ:

``` text
┌─parseDateTimeBestEffortUS─┐
│     2020-09-12 12:12:57   │
└─────────────────────────——┘
```

Запрос:

``` sql
SELECT parseDateTimeBestEffortUS('09.12.2020 12:12:57')
AS parseDateTimeBestEffortUS;
```

Ответ:

``` text
┌─parseDateTimeBestEffortUS─┐
│     2020-09-12 12:12:57   │
└─────────────────────────——┘
```

## toUnixTimestamp64Milli
## toUnixTimestamp64Micro
## toUnixTimestamp64Nano

Преобразует значение `DateTime64` в значение `Int64` с фиксированной точностью менее одной секунды. 
Входное значение округляется соответствующим образом вверх или вниз в зависимости от его точности. Обратите внимание, что возвращаемое значение - это временная метка в UTC, а не в часовом поясе `DateTime64`.

**Синтаксис**

``` sql
toUnixTimestamp64Milli(value)
```

**Параметры**

-   `value` — значение `DateTime64` с любой точностью.

**Возвращаемое значение**

-   Значение `value`, преобразованное в тип данных `Int64`.

**Примеры**

Запрос:

``` sql
WITH toDateTime64('2019-09-16 19:20:12.345678910', 6) AS dt64
SELECT toUnixTimestamp64Milli(dt64)
```

Ответ:

``` text
┌─toUnixTimestamp64Milli(dt64)─┐
│                1568650812345 │
└──────────────────────────────┘
```

Запрос: 

``` sql
WITH toDateTime64('2019-09-16 19:20:12.345678910', 6) AS dt64
SELECT toUnixTimestamp64Nano(dt64)
```

Ответ:

``` text
┌─toUnixTimestamp64Nano(dt64)─┐
│         1568650812345678000 │
└─────────────────────────────┘
```

## fromUnixTimestamp64Milli
## fromUnixTimestamp64Micro
## fromUnixTimestamp64Nano

Преобразует значение `Int64` в значение `DateTime64` с фиксированной точностью менее одной секунды и дополнительным часовым поясом. Входное значение округляется соответствующим образом вверх или вниз в зависимости от его точности. Обратите внимание, что входное значение обрабатывается как метка времени UTC, а не метка времени в заданном (или неявном) часовом поясе.

**Синтаксис**

``` sql
fromUnixTimestamp64Milli(value [, ti])
```

**Параметры**

-   `value` — значение типы `Int64` с любой точностью.
-   `timezone` — (не обязательный параметр) часовой пояс в формате `String` для возвращаемого результата.

**Возвращаемое значение**

-   Значение `value`, преобразованное в тип данных `DateTime64`.

**Пример**

Запрос:

``` sql
WITH CAST(1234567891011, 'Int64') AS i64
SELECT fromUnixTimestamp64Milli(i64, 'UTC')
```

Ответ:

``` text
┌─fromUnixTimestamp64Milli(i64, 'UTC')─┐
│              2009-02-13 23:31:31.011 │
└──────────────────────────────────────┘
```

## toLowCardinality {#tolowcardinality}

Преобразует входные данные в версию [LowCardianlity](../data-types/lowcardinality.md) того же типа данных.

Чтобы преобразовать данные из типа `LowCardinality`, используйте функцию [CAST](#type_conversion_function-cast). Например, `CAST(x as String)`.

**Синтаксис**

```sql
toLowCardinality(expr)
```

**Параметры**

- `expr` — [Выражение](../syntax.md#syntax-expressions), которое в результате преобразуется в один из [поддерживаемых типов данных](../data-types/index.md#data_types).


**Возвращаемое значение**

- Результат преобразования `expr`.

Тип: `LowCardinality(expr_result_type)`

**Example**

Запрос:

```sql
SELECT toLowCardinality('1')
```

Результат:

```text
┌─toLowCardinality('1')─┐
│ 1                     │
└───────────────────────┘
```

## formatRow {#formatrow}

Преобразует произвольные выражения в строку заданного формата.

**Синтаксис** 

``` sql
formatRow(format, x, y, ...)
```

**Параметры**

-   `format` — Текстовый формат. Например, [CSV](../../interfaces/formats.md#csv), [TSV](../../interfaces/formats.md#tabseparated).
-   `x`,`y`, ... — Выражения.

**Возвращаемое значение**

-   Отформатированная строка (в текстовых форматах обычно с завершающим переводом строки).

**Пример**

Запрос:

``` sql
SELECT formatRow('CSV', number, 'good')
FROM numbers(3)
```

Ответ:

``` text
┌─formatRow('CSV', number, 'good')─┐
│ 0,"good"
                         │
│ 1,"good"
                         │
│ 2,"good"
                         │
└──────────────────────────────────┘
```

## formatRowNoNewline {#formatrownonewline}

Преобразует произвольные выражения в строку заданного формата. При этом удаляет лишние переводы строк `\n`, если они появились.

**Синтаксис** 

``` sql
formatRowNoNewline(format, x, y, ...)
```

**Параметры**

-   `format` — Текстовый формат. Например, [CSV](../../interfaces/formats.md#csv), [TSV](../../interfaces/formats.md#tabseparated).
-   `x`,`y`, ... — Выражения.

**Возвращаемое значение**

-   Отформатированная строка (в текстовых форматах без завершающего перевода строки).

**Пример**

Запрос:

``` sql
SELECT formatRowNoNewline('CSV', number, 'good')
FROM numbers(3)
```

Ответ:

``` text
┌─formatRowNoNewline('CSV', number, 'good')─┐
│ 0,"good"                                  │
│ 1,"good"                                  │
│ 2,"good"                                  │
└───────────────────────────────────────────┘
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/functions/type_conversion_functions/) <!--hide-->
