---
slug: /ru/sql-reference/functions/type-conversion-functions
sidebar_position: 38
sidebar_label: "Функции преобразования типов"
---

# Функции преобразования типов {#type-conversion-functions}

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

**Аргументы**

-   `expr` — [выражение](../syntax.md#syntax-expressions) возвращающее число или строку с десятичным представление числа. Бинарное, восьмеричное и шестнадцатеричное представление числа не поддержаны. Ведущие нули обрезаются.

**Возвращаемое значение**

Целое число типа `Int8`, `Int16`, `Int32`, `Int64`, `Int128` или `Int256`.

Функции используют [округление к нулю](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), т.е. обрезают дробную часть числа.

Поведение функций для аргументов [NaN и Inf](../../sql-reference/functions/type-conversion-functions.md#data_type-float-nan-inf) не определено. При использовании функций помните о возможных проблемах при [преобразовании чисел](#numeric-conversion-issues).

**Пример**

Запрос:

``` sql
SELECT toInt64(nan), toInt32(32), toInt16('16'), toInt8(8.8);
```

Результат:

``` text
┌─────────toInt64(nan)─┬─toInt32(32)─┬─toInt16('16')─┬─toInt8(8.8)─┐
│ -9223372036854775808 │          32 │            16 │           8 │
└──────────────────────┴─────────────┴───────────────┴─────────────┘
```

## toInt(8\|16\|32\|64\|128\|256)OrZero {#toint8163264orzero}

Принимает аргумент типа String и пытается его распарсить в Int(8\|16\|32\|64\|128\|256). Если не удалось - возвращает 0.

**Пример**

Запрос:

``` sql
SELECT toInt64OrZero('123123'), toInt8OrZero('123qwe123');
```

Результат:

``` text
┌─toInt64OrZero('123123')─┬─toInt8OrZero('123qwe123')─┐
│                  123123 │                         0 │
└─────────────────────────┴───────────────────────────┘
```

## toInt(8\|16\|32\|64\|128\|256)OrNull {#toint8163264ornull}

Принимает аргумент типа String и пытается его распарсить в Int(8\|16\|32\|64\|128\|256). Если не удалось - возвращает NULL.

**Пример**

Запрос:

``` sql
SELECT toInt64OrNull('123123'), toInt8OrNull('123qwe123');
```

Результат:

``` text
┌─toInt64OrNull('123123')─┬─toInt8OrNull('123qwe123')─┐
│                  123123 │                      ᴺᵁᴸᴸ │
└─────────────────────────┴───────────────────────────┘
```

## toInt(8\|16\|32\|64\|128\|256)OrDefault {#toint8163264128256orDefault}

Принимает аргумент типа String и пытается его распарсить в Int(8\|16\|32\|64\|128\|256). Если не удалось —  возвращает значение по умолчанию.

**Пример**

Запрос:

``` sql
SELECT toInt64OrDefault('123123', cast('-1' as Int64)), toInt8OrDefault('123qwe123', cast('-1' as Int8));
```

Результат:

``` text
┌─toInt64OrDefault('123123', CAST('-1', 'Int64'))─┬─toInt8OrDefault('123qwe123', CAST('-1', 'Int8'))─┐
│                                          123123 │                                               -1 │
└─────────────────────────────────────────────────┴──────────────────────────────────────────────────┘
```

## toUInt(8\|16\|32\|64\|256) {#touint8163264}

Преобраует входное значение к типу [UInt](../../sql-reference/functions/type-conversion-functions.md). Семейство функций включает:

-   `toUInt8(expr)` — возвращает значение типа `UInt8`.
-   `toUInt16(expr)` — возвращает значение типа `UInt16`.
-   `toUInt32(expr)` — возвращает значение типа `UInt32`.
-   `toUInt64(expr)` — возвращает значение типа `UInt64`.
-   `toUInt256(expr)` — возвращает значение типа `UInt256`.

**Аргументы**

-   `expr` — [выражение](../syntax.md#syntax-expressions) возвращающее число или строку с десятичным представление числа. Бинарное, восьмеричное и шестнадцатеричное представление числа не поддержаны. Ведущие нули обрезаются.

**Возвращаемое значение**

Целое число типа `UInt8`, `UInt16`, `UInt32`, `UInt64` или `UInt256`.

Функции используют [округление к нулю](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), т.е. обрезают дробную часть числа.

Поведение функций для аргументов [NaN и Inf](../../sql-reference/functions/type-conversion-functions.md#data_type-float-nan-inf) не определено. Если передать строку, содержащую отрицательное число, например `'-32'`, ClickHouse генерирует исключение. При использовании функций помните о возможных проблемах при [преобразовании чисел](#numeric-conversion-issues).

**Пример**

Запрос:

``` sql
SELECT toUInt64(nan), toUInt32(-32), toUInt16('16'), toUInt8(8.8);
```

Результат:

``` text
┌───────toUInt64(nan)─┬─toUInt32(-32)─┬─toUInt16('16')─┬─toUInt8(8.8)─┐
│ 9223372036854775808 │    4294967264 │             16 │            8 │
└─────────────────────┴───────────────┴────────────────┴──────────────┘
```

## toUInt(8\|16\|32\|64\|256)OrZero {#touint8163264orzero}

## toUInt(8\|16\|32\|64\|256)OrNull {#touint8163264ornull}

## toUInt(8\|16\|32\|64\|256)OrDefault {#touint8163264256ordefault}

## toFloat(32\|64) {#tofloat3264}

## toFloat(32\|64)OrZero {#tofloat3264orzero}

## toFloat(32\|64)OrNull {#tofloat3264ornull}

## toFloat(32\|64)OrDefault {#tofloat3264ordefault}

## toDate {#todate}

Конвертирует аргумент в значение [Date](/docs/ru/sql-reference/data-types/date.md).

**Синтаксис**

``` sql
toDate(expr)
```

**Аргументы**

- `expr` — Значение для преобразования. [String](/docs/ru/sql-reference/data-types/string.md), [Int](/docs/ru/sql-reference/data-types/int-uint.md), [Date](/docs/ru/sql-reference/data-types/date.md) или [DateTime](/docs/ru/sql-reference/data-types/datetime.md).

Если `expr` является числом выглядит как UNIX timestamp (больше чем 65535), оно интерпретируется как DateTime, затем обрезается до Date учитывавая текущую часовой пояс. Если `expr` является числом и меньше чем 65536, оно интерпретируется как количество дней с 1970-01-01.

**Возвращаемое значение**

- Календарная дата. [Date](/docs/ru/sql-reference/data-types/date.md).

**Пример**

Запрос:

``` sql
SELECT toDate('2022-12-30'), toDate(1685457500);
```

Результат:

```response
┌─toDate('2022-12-30')─┬─toDate(1685457500)─┐
│           2022-12-30 │         2023-05-30 │
└──────────────────────┴────────────────────┘
```


## toDateOrZero {#todateorzero}

Как [toDate](#todate), но в случае неудачи возвращает нижнюю границу [Date](/docs/ru/sql-reference/data-types/date.md)). Поддерживается только аргумент типа [String](/docs/ru/sql-reference/data-types/string.md).

**Пример**

Запрос:

``` sql
SELECT toDateOrZero('2022-12-30'), toDateOrZero('');
```

Результат:

```response
┌─toDateOrZero('2022-12-30')─┬─toDateOrZero('')─┐
│                 2022-12-30 │       1970-01-01 │
└────────────────────────────┴──────────────────┘
```


## toDateOrNull {#todateornull}

Как [toDate](#todate), но в случае неудачи возвращает `NULL`. Поддерживается только аргумент типа [String](/docs/ru/sql-reference/data-types/string.md).

**Пример**

Запрос:

``` sql
SELECT toDateOrNull('2022-12-30'), toDateOrNull('');
```

Результат:

```response
┌─toDateOrNull('2022-12-30')─┬─toDateOrNull('')─┐
│                 2022-12-30 │             ᴺᵁᴸᴸ │
└────────────────────────────┴──────────────────┘
```


## toDateOrDefault {#todateordefault}

Как [toDate](#todate), но в случае неудачи возвращает значение по умолчанию (или второй аргумент (если указан), или нижняя граница [Date](/docs/ru/sql-reference/data-types/date.md)).

**Синтаксис**

``` sql
toDateOrDefault(expr [, default_value])
```

**Пример**

Запрос:

``` sql
SELECT toDateOrDefault('2022-12-30'), toDateOrDefault('', '2023-01-01'::Date);
```

Результат:

```response
┌─toDateOrDefault('2022-12-30')─┬─toDateOrDefault('', CAST('2023-01-01', 'Date'))─┐
│                    2022-12-30 │                                      2023-01-01 │
└───────────────────────────────┴─────────────────────────────────────────────────┘
```


## toDateTime {#todatetime}

Конвертирует аргумент в значение [DateTime](/docs/ru/sql-reference/data-types/datetime.md).

**Синтаксис**

``` sql
toDateTime(expr[, time_zone ])
```

**Аргументы**

- `expr` — Значение для преобразования. [String](/docs/ru/sql-reference/data-types/string.md), [Int](/docs/ru/sql-reference/data-types/int-uint.md), [Date](/docs/ru/sql-reference/data-types/date.md) или [DateTime](/docs/ru/sql-reference/data-types/datetime.md).
- `time_zone` — Часовой пояс. [String](/docs/ru/sql-reference/data-types/string.md).

:::note
Если `expr` является числом, то оно интерпретируется как число секунд с начала Unix-эпохи (Unix Timestamp).

Если же `expr` -- [строка (String)](/docs/ru/sql-reference/data-types/string.md), то оно может быть интерпретировано и как Unix Timestamp, и как строковое представление даты / даты со временем.  
Ввиду неоднозначности запрещён парсинг строк длиной 4 и меньше. Так, строка `'1999'` могла бы представлять собой как год (неполное строковое представление даты или даты со временем), так и Unix Timestamp.  
Строки длиной 5 символов и более не несут неоднозначности, а следовательно, их парсинг разрешён.
:::

**Возвращаемое значение**

- Время. [DateTime](/docs/ru/sql-reference/data-types/datetime.md)

**Пример**

Запрос:

``` sql
SELECT toDateTime('2022-12-30 13:44:17'), toDateTime(1685457500, 'UTC');
```

Результат:

```response
┌─toDateTime('2022-12-30 13:44:17')─┬─toDateTime(1685457500, 'UTC')─┐
│               2022-12-30 13:44:17 │           2023-05-30 14:38:20 │
└───────────────────────────────────┴───────────────────────────────┘
```


## toDateTimeOrZero {#todatetimeorzero}

Как [toDateTime](#todatetime), но в случае неудачи возвращает нижнюю границу [DateTime](/docs/ru/sql-reference/data-types/datetime.md)). Поддерживается только аргумент типа [String](/docs/ru/sql-reference/data-types/string.md).

**Пример**

Запрос:

``` sql
SELECT toDateTimeOrZero('2022-12-30 13:44:17'), toDateTimeOrZero('');
```

Результат:

```response
┌─toDateTimeOrZero('2022-12-30 13:44:17')─┬─toDateTimeOrZero('')─┐
│                     2022-12-30 13:44:17 │  1970-01-01 00:00:00 │
└─────────────────────────────────────────┴──────────────────────┘
```


## toDateTimeOrNull {#todatetimeornull}

Как [toDateTime](#todatetime), но в случае неудачи возвращает `NULL`. Поддерживается только аргумент типа [String](/docs/ru/sql-reference/data-types/string.md).

**Example**

Query:

``` sql
SELECT toDateTimeOrNull('2022-12-30 13:44:17'), toDateTimeOrNull('');
```

Result:

```response
┌─toDateTimeOrNull('2022-12-30 13:44:17')─┬─toDateTimeOrNull('')─┐
│                     2022-12-30 13:44:17 │                 ᴺᵁᴸᴸ │
└─────────────────────────────────────────┴──────────────────────┘
```


## toDateTimeOrDefault {#todatetimeordefault}

Как [toDateTime](#todatetime), но в случае неудачи возвращает значение по умолчанию (или третий аргумент (если указан), или нижняя граница [DateTime](/docs/ru/sql-reference/data-types/datetime.md)).

**Синтаксис**

``` sql
toDateTimeOrDefault(expr, [, time_zone [, default_value]])
```

**Пример**

Запрос:

``` sql
SELECT toDateTimeOrDefault('2022-12-30 13:44:17'), toDateTimeOrDefault('', 'UTC', '2023-01-01'::DateTime('UTC'));
```

Результат:

```response
┌─toDateTimeOrDefault('2022-12-30 13:44:17')─┬─toDateTimeOrDefault('', 'UTC', CAST('2023-01-01', 'DateTime(\'UTC\')'))─┐
│                        2022-12-30 13:44:17 │                                                     2023-01-01 00:00:00 │
└────────────────────────────────────────────┴─────────────────────────────────────────────────────────────────────────┘
```


## toDate32 {#todate32}

Конвертирует аргумент в значение типа [Date32](../../sql-reference/data-types/date32.md). Если значение выходит за границы диапазона, возвращается пограничное значение `Date32`. Если аргумент имеет тип [Date](../../sql-reference/data-types/date.md), учитываются границы типа `Date`.

**Синтаксис**

``` sql
toDate32(value)
```

**Аргументы**

-   `value` — Значение даты. [String](../../sql-reference/data-types/string.md), [UInt32](../../sql-reference/data-types/int-uint.md) или [Date](../../sql-reference/data-types/date.md).

**Возвращаемое значение**

-   Календарная дата.

Тип: [Date32](../../sql-reference/data-types/date32.md).

**Пример**

1. Значение находится в границах диапазона:

``` sql
SELECT toDate32('1955-01-01') AS value, toTypeName(value);
```

``` text
┌──────value─┬─toTypeName(toDate32('1955-01-01'))─┐
│ 1955-01-01 │ Date32                             │
└────────────┴────────────────────────────────────┘
```

2. Значение выходит за границы диапазона:

``` sql
SELECT toDate32('1899-01-01') AS value, toTypeName(value);
```

``` text
┌──────value─┬─toTypeName(toDate32('1899-01-01'))─┐
│ 1900-01-01 │ Date32                             │
└────────────┴────────────────────────────────────┘
```

3. С аргументом типа `Date`:

``` sql
SELECT toDate32(toDate('1899-01-01')) AS value, toTypeName(value);
```

``` text
┌──────value─┬─toTypeName(toDate32(toDate('1899-01-01')))─┐
│ 1970-01-01 │ Date32                                     │
└────────────┴────────────────────────────────────────────┘
```

## toDate32OrZero {#todate32-or-zero}

То же самое, что и  [toDate32](#todate32), но возвращает минимальное значение типа [Date32](../../sql-reference/data-types/date32.md), если получен недопустимый аргумент.

**Пример**

Запрос:

``` sql
SELECT toDate32OrZero('1899-01-01'), toDate32OrZero('');
```

Результат:

``` text
┌─toDate32OrZero('1899-01-01')─┬─toDate32OrZero('')─┐
│                   1900-01-01 │         1900-01-01 │
└──────────────────────────────┴────────────────────┘
```

## toDate32OrNull {#todate32-or-null}

То же самое, что и [toDate32](#todate32), но возвращает `NULL`, если получен недопустимый аргумент.

**Пример**

Запрос:

``` sql
SELECT toDate32OrNull('1955-01-01'), toDate32OrNull('');
```

Результат:

``` text
┌─toDate32OrNull('1955-01-01')─┬─toDate32OrNull('')─┐
│                   1955-01-01 │               ᴺᵁᴸᴸ │
└──────────────────────────────┴────────────────────┘
```

## toDate32OrDefault {#todate32-or-default}

Конвертирует аргумент в значение типа [Date32](../../sql-reference/data-types/date32.md). Если значение выходит за границы диапазона, возвращается нижнее пограничное значение `Date32`. Если аргумент имеет тип [Date](../../sql-reference/data-types/date.md), учитываются границы типа `Date`. Возвращает значение по умолчанию, если получен недопустимый аргумент.

**Пример**

Запрос:

``` sql
SELECT
    toDate32OrDefault('1930-01-01', toDate32('2020-01-01')),
    toDate32OrDefault('xx1930-01-01', toDate32('2020-01-01'));
```

Результат:

``` text
┌─toDate32OrDefault('1930-01-01', toDate32('2020-01-01'))─┬─toDate32OrDefault('xx1930-01-01', toDate32('2020-01-01'))─┐
│                                              1930-01-01 │                                                2020-01-01 │
└─────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────┘
```

## toDateTime64

## toDateTime64OrZero

## toDateTime64OrNull

## toDateTime64OrDefault

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

**Аргументы**

-   `expr` — [выражение](../syntax.md#syntax-expressions), возвращающее значение типа [String](../../sql-reference/functions/type-conversion-functions.md). ClickHouse ожидает текстовое представление десятичного числа. Например, `'1.111'`.
-   `S` — количество десятичных знаков в результирующем значении.

**Возвращаемое значение**

Значение типа `Nullable(Decimal(P,S))`. Значение содержит:

-   Число с `S` десятичными знаками, если ClickHouse распознал число во входной строке.
-   `NULL`, если ClickHouse не смог распознать число во входной строке или входное число содержит больше чем `S` десятичных знаков.

**Примеры**

Запрос:

``` sql
SELECT toDecimal32OrNull(toString(-1.111), 5) AS val, toTypeName(val);
```

Результат:

``` text
┌────val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 5))─┐
│ -1.111 │ Nullable(Decimal(9, 5))                            │
└────────┴────────────────────────────────────────────────────┘
```

Запрос:

``` sql
SELECT toDecimal32OrNull(toString(-1.111), 2) AS val, toTypeName(val);
```

Результат:

``` text
┌──val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 2))─┐
│ ᴺᵁᴸᴸ │ Nullable(Decimal(9, 2))                            │
└──────┴────────────────────────────────────────────────────┘
```

## toDecimal(32\|64\|128\|256)OrDefault {#todecimal3264128256ordefault}

Преобразует входную строку в значение с типом данных [Decimal(P,S)](../../sql-reference/data-types/decimal.md). Семейство функций включает в себя:

-   `toDecimal32OrDefault(expr, S)` — возвращает значение типа `Decimal32(S)`.
-   `toDecimal64OrDefault(expr, S)` — возвращает значение типа `Decimal64(S)`.
-   `toDecimal128OrDefault(expr, S)` — возвращает значение типа `Decimal128(S)`.
-   `toDecimal256OrDefault(expr, S)` — возвращает значение типа `Decimal256(S)`.

Эти функции следует использовать вместо функций `toDecimal*()`, если при ошибке обработки входного значения вы хотите получать значение по умолчанию вместо исключения.

**Аргументы**

-   `expr` — [выражение](../syntax.md#syntax-expressions), возвращающее значение типа [String](../../sql-reference/functions/type-conversion-functions.md). ClickHouse ожидает текстовое представление десятичного числа. Например, `'1.111'`.
-   `S` — количество десятичных знаков в результирующем значении.

**Возвращаемое значение**

Значение типа `Decimal(P,S)`. Значение содержит:

-   Число с `S` десятичными знаками, если ClickHouse распознал число во входной строке.
-   Значение по умолчанию типа `Decimal(P,S)`, если ClickHouse не смог распознать число во входной строке или входное число содержит больше чем `S` десятичных знаков.

**Примеры**

Запрос:

``` sql
SELECT toDecimal32OrDefault(toString(-1.111), 5) AS val, toTypeName(val);
```

Результат:

``` text
┌────val─┬─toTypeName(toDecimal32OrDefault(toString(-1.111), 5))─┐
│ -1.111 │ Decimal(9, 5)                                         │
└────────┴───────────────────────────────────────────────────────┘
```

Запрос:

``` sql
SELECT toDecimal32OrDefault(toString(-1.111), 2) AS val, toTypeName(val);
```

Результат:

``` text
┌─val─┬─toTypeName(toDecimal32OrDefault(toString(-1.111), 2))─┐
│   0 │ Decimal(9, 2)                                         │
└─────┴───────────────────────────────────────────────────────┘
```

## toDecimal(32\|64\|128\|256)OrZero {#todecimal3264128orzero}

Преобразует тип входного значения в [Decimal (P, S)](../../sql-reference/functions/type-conversion-functions.md). Семейство функций включает в себя:

-   `toDecimal32OrZero( expr, S)` — возвращает значение типа `Decimal32(S)`.
-   `toDecimal64OrZero( expr, S)` — возвращает значение типа `Decimal64(S)`.
-   `toDecimal128OrZero( expr, S)` — возвращает значение типа `Decimal128(S)`.
-   `toDecimal256OrZero( expr, S)` — возвращает значение типа `Decimal256(S)`.

Эти функции следует использовать вместо функций `toDecimal*()`, если при ошибке обработки входного значения вы хотите получать `0` вместо исключения.

**Аргументы**

-   `expr` — [выражение](../syntax.md#syntax-expressions), возвращающее значение типа [String](../../sql-reference/functions/type-conversion-functions.md). ClickHouse ожидает текстовое представление десятичного числа. Например, `'1.111'`.
-   `S` — количество десятичных знаков в результирующем значении.

**Возвращаемое значение**

Значение типа `Nullable(Decimal(P,S))`. `P` равно числовой части имени функции. Например, для функции `toDecimal32OrZero`, `P = 32`. Значение содержит:

-   Число с `S` десятичными знаками, если ClickHouse распознал число во входной строке.
-   0 c `S` десятичными знаками, если ClickHouse не смог распознать число во входной строке или входное число содержит больше чем `S` десятичных знаков.

**Пример**

Запрос:

``` sql
SELECT toDecimal32OrZero(toString(-1.111), 5) AS val, toTypeName(val);
```

Результат:

``` text
┌────val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 5))─┐
│ -1.111 │ Decimal(9, 5)                                      │
└────────┴────────────────────────────────────────────────────┘
```

Запрос:

``` sql
SELECT toDecimal32OrZero(toString(-1.111), 2) AS val, toTypeName(val);
```

Результат:

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

**Пример**

Запрос:

``` sql
SELECT
    now() AS now_local,
    toString(now(), 'Asia/Yekaterinburg') AS now_yekat;
```

Результат:

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

**Примеры**

Запрос:

``` sql
SELECT toFixedString('foo', 8) AS s, toStringCutToZero(s) AS s_cut;
```

Результат:

``` text
┌─s─────────────┬─s_cut─┐
│ foo\0\0\0\0\0 │ foo   │
└───────────────┴───────┘
```

Запрос:

``` sql
SELECT toFixedString('foo\0bar', 8) AS s, toStringCutToZero(s) AS s_cut;
```

Результат:

``` text
┌─s──────────┬─s_cut─┐
│ foo\0bar\0 │ foo   │
└────────────┴───────┘
```

## toDecimalString

Принимает любой численный тип первым аргументом, возвращает строковое десятичное представление числа с точностью, заданной вторым аргументом.

**Синтаксис**

``` sql
toDecimalString(number, scale)
```

**Параметры**

-   `number` — Значение любого числового типа: [Int, UInt](/docs/ru/sql-reference/data-types/int-uint.md), [Float](/docs/ru/sql-reference/data-types/float.md), [Decimal](/docs/ru/sql-reference/data-types/decimal.md),
-   `scale` — Требуемое количество десятичных знаков после запятой, [UInt8](/docs/ru/sql-reference/data-types/int-uint.md).
    * Значение `scale` для типов [Decimal](/docs/ru/sql-reference/data-types/decimal.md) и [Int, UInt](/docs/ru/sql-reference/data-types/int-uint.md) должно не превышать 77 (так как это наибольшее количество значимых символов для этих типов),
    * Значение `scale` для типа [Float](/docs/ru/sql-reference/data-types/float.md) не должно превышать 60.

**Возвращаемое значение**

-   Строка ([String](/docs/en/sql-reference/data-types/string.md)), представляющая собой десятичное представление входного числа с заданной длиной дробной части.
    При необходимости число округляется по стандартным правилам арифметики.

**Пример использования**

Запрос:

``` sql
SELECT toDecimalString(CAST('64.32', 'Float64'), 5);
```

Результат:

```response
┌─toDecimalString(CAST('64.32', 'Float64'), 5)┐
│ 64.32000                                    │
└─────────────────────────────────────────────┘
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

Функция принимает строку из 16 байт и интерпретирует ее байты в порядок от старшего к младшему. Если строка имеет недостаточную длину, то функция работает так, как будто строка дополнена необходимым количеством нулевых байтов с конца. Если строка длиннее, чем 16 байтов, то лишние байты с конца игнорируются.

**Синтаксис**

``` sql
reinterpretAsUUID(fixed_string)
```

**Аргументы**

-   `fixed_string` — cтрока с big-endian порядком байтов. [FixedString](../../sql-reference/data-types/fixedstring.md#fixedstring).

**Возвращаемое значение**

-   Значение типа [UUID](../../sql-reference/data-types/uuid.md#uuid-data-type).

**Примеры**

Интерпретация строки как UUID.

Запрос:

``` sql
SELECT reinterpretAsUUID(reverse(unhex('000102030405060708090a0b0c0d0e0f')));
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

## reinterpret(x, T) {#type_conversion_function-reinterpret}

Использует ту же самую исходную последовательность байтов в памяти для значения `x` и интерпретирует ее как конечный тип данных `T`.

**Синтаксис**

``` sql
reinterpret(x, type)
```

**Аргументы**

-   `x` — любой тип данных.
-   `type` — конечный тип данных. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   Значение конечного типа данных.

**Примеры**

Запрос:

```sql
SELECT reinterpret(toInt8(-1), 'UInt8') as int_to_uint,
    reinterpret(toInt8(1), 'Float32') as int_to_float,
    reinterpret('1', 'UInt32') as string_to_int;
```

Результат:

```
┌─int_to_uint─┬─int_to_float─┬─string_to_int─┐
│         255 │        1e-45 │            49 │
└─────────────┴──────────────┴───────────────┘
```

## CAST(x, T) {#type_conversion_function-cast}

Преобразует входное значение к указанному типу данных. В отличие от функции [reinterpret](#type_conversion_function-reinterpret) `CAST` пытается представить то же самое значение в новом типе данных. Если преобразование невозможно, то возникает исключение.
Поддерживается несколько вариантов синтаксиса.

**Синтаксис**

``` sql
CAST(x, T)
CAST(x AS t)
x::t
```

**Аргументы**

-   `x` — значение, которое нужно преобразовать. Может быть любого типа.
-   `T` — имя типа данных. [String](../../sql-reference/data-types/string.md).
-   `t` — тип данных.

**Возвращаемое значение**

-   Преобразованное значение.

:::note Примечание
Если входное значение выходит за границы нового типа, то результат переполняется. Например, `CAST(-1, 'UInt8')` возвращает `255`.
:::

**Примеры**

Запрос:

```sql
SELECT
    CAST(toInt8(-1), 'UInt8') AS cast_int_to_uint,
    CAST(1.5 AS Decimal(3,2)) AS cast_float_to_decimal,
    '1'::Int32 AS cast_string_to_int;
```

Результат:

```
┌─cast_int_to_uint─┬─cast_float_to_decimal─┬─cast_string_to_int─┐
│              255 │                  1.50 │                  1 │
└──────────────────┴───────────────────────┴────────────────────┘
```

Запрос:

``` sql
SELECT
    '2016-06-15 23:00:00' AS timestamp,
    CAST(timestamp AS DateTime) AS datetime,
    CAST(timestamp AS Date) AS date,
    CAST(timestamp, 'String') AS string,
    CAST(timestamp, 'FixedString(22)') AS fixed_string;
```

Результат:

``` text
┌─timestamp───────────┬────────────datetime─┬───────date─┬─string──────────────┬─fixed_string──────────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00\0\0\0 │
└─────────────────────┴─────────────────────┴────────────┴─────────────────────┴───────────────────────────┘
```

Преобразование в FixedString(N) работает только для аргументов типа [String](../../sql-reference/data-types/string.md) или [FixedString](../../sql-reference/data-types/fixedstring.md).

Поддерживается преобразование к типу [Nullable](../../sql-reference/data-types/nullable.md) и обратно.

**Примеры**

Запрос:

``` sql
SELECT toTypeName(x) FROM t_null;
```

Результат:

``` text
┌─toTypeName(x)─┐
│ Int8          │
│ Int8          │
└───────────────┘
```

Запрос:

``` sql
SELECT toTypeName(CAST(x, 'Nullable(UInt16)')) FROM t_null;
```

Результат:

``` text
┌─toTypeName(CAST(x, 'Nullable(UInt16)'))─┐
│ Nullable(UInt16)                        │
│ Nullable(UInt16)                        │
└─────────────────────────────────────────┘
```

**Смотрите также**

-   Настройка [cast_keep_nullable](../../operations/settings/settings.md#cast_keep_nullable)

## accurateCast(x, T) {#type_conversion_function-accurate-cast}

Преобразует входное значение `x` в указанный тип данных `T`.

В отличие от функции [cast(x, T)](#type_conversion_function-cast), `accurateCast` не допускает переполнения при преобразовании числовых типов. Например, `accurateCast(-1, 'UInt8')` вызовет исключение.

**Примеры**

Запрос:

``` sql
SELECT cast(-1, 'UInt8') as uint8;
```

Результат:

``` text
┌─uint8─┐
│   255 │
└───────┘
```

Запрос:

```sql
SELECT accurateCast(-1, 'UInt8') as uint8;
```

Результат:

``` text
Code: 70. DB::Exception: Received from localhost:9000. DB::Exception: Value in column Int8 cannot be safely converted into type UInt8: While processing accurateCast(-1, 'UInt8') AS uint8.
```

## accurateCastOrNull(x, T) {#type_conversion_function-accurate-cast_or_null}

Преобразует входное значение `x` в указанный тип данных `T`.

Всегда возвращает тип [Nullable](../../sql-reference/data-types/nullable.md). Если исходное значение не может быть преобразовано к целевому типу, возвращает [NULL](../../sql-reference/syntax.md#null-literal).

**Синтаксис**

```sql
accurateCastOrNull(x, T)
```

**Аргументы**

-   `x` — входное значение.
-   `T` — имя возвращаемого типа данных.

**Возвращаемое значение**

-   Значение, преобразованное в указанный тип `T`.

**Примеры**

Запрос:

``` sql
SELECT toTypeName(accurateCastOrNull(5, 'UInt8'));
```

Результат:

``` text
┌─toTypeName(accurateCastOrNull(5, 'UInt8'))─┐
│ Nullable(UInt8)                            │
└────────────────────────────────────────────┘
```

Запрос:

``` sql
SELECT
    accurateCastOrNull(-1, 'UInt8') as uint8,
    accurateCastOrNull(128, 'Int8') as int8,
    accurateCastOrNull('Test', 'FixedString(2)') as fixed_string;
```

Результат:

``` text
┌─uint8─┬─int8─┬─fixed_string─┐
│  ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │
└───────┴──────┴──────────────┘
```


## accurateCastOrDefault(x, T[, default_value]) {#type_conversion_function-accurate-cast_or_default}

Преобразует входное значение `x` в указанный тип данных `T`. Если исходное значение не может быть преобразовано к целевому типу, возвращает значение по умолчанию или `default_value`, если оно указано.

**Синтаксис**

```sql
accurateCastOrDefault(x, T)
```

**Аргументы**

-   `x` — входное значение.
-   `T` — имя возвращаемого типа данных.
-   `default_value` — значение по умолчанию возвращаемого типа данных.

**Возвращаемое значение**

-   Значение, преобразованное в указанный тип `T`.

**Пример**

Запрос:

``` sql
SELECT toTypeName(accurateCastOrDefault(5, 'UInt8'));
```

Результат:

``` text
┌─toTypeName(accurateCastOrDefault(5, 'UInt8'))─┐
│ UInt8                                         │
└───────────────────────────────────────────────┘
```

Запрос:

``` sql
SELECT
    accurateCastOrDefault(-1, 'UInt8') as uint8,
    accurateCastOrDefault(-1, 'UInt8', 5) as uint8_default,
    accurateCastOrDefault(128, 'Int8') as int8,
    accurateCastOrDefault(128, 'Int8', 5) as int8_default,
    accurateCastOrDefault('Test', 'FixedString(2)') as fixed_string,
    accurateCastOrDefault('Test', 'FixedString(2)', 'Te') as fixed_string_default;
```

Результат:

``` text
┌─uint8─┬─uint8_default─┬─int8─┬─int8_default─┬─fixed_string─┬─fixed_string_default─┐
│     0 │             5 │    0 │            5 │              │ Te                   │
└───────┴───────────────┴──────┴──────────────┴──────────────┴──────────────────────┘
```

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

**Аргументы**

-   `number` — длительность интервала. Положительное целое число.

**Возвращаемые значения**

-   Значение с типом данных `Interval`.

**Пример**

Запрос:

``` sql
WITH
    toDate('2019-01-01') AS date,
    INTERVAL 1 WEEK AS interval_week,
    toIntervalWeek(1) AS interval_to_week
SELECT
    date + interval_week,
    date + interval_to_week;
```

Результат:

``` text
┌─plus(date, interval_week)─┬─plus(date, interval_to_week)─┐
│                2019-01-08 │                   2019-01-08 │
└───────────────────────────┴──────────────────────────────┘
```

## parseDateTimeBestEffort {#parsedatetimebesteffort}
## parseDateTime32BestEffort {#parsedatetime32besteffort}

Преобразует дату и время в [строковом](../../sql-reference/functions/type-conversion-functions.md) представлении к типу данных [DateTime](../../sql-reference/functions/type-conversion-functions.md#data_type-datetime).

Функция распознаёт форматы [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), [RFC 1123 - 5.2.14 RFC-822 Date and Time Specification](https://tools.ietf.org/html/rfc1123#page-55), формат даты времени ClickHouse’s а также некоторые другие форматы.

**Синтаксис**

``` sql
parseDateTimeBestEffort(time_string[, time_zone])
```

**Аргументы**

-   `time_string` — строка, содержащая дату и время для преобразования. [String](../../sql-reference/functions/type-conversion-functions.md).
-   `time_zone` — часовой пояс. Функция анализирует `time_string` в соответствии с заданным часовым поясом. [String](../../sql-reference/functions/type-conversion-functions.md).

**Поддерживаемые нестандартные форматы**

-   [Unix timestamp](https://ru.wikipedia.org/wiki/Unix-время) в строковом представлении. 9 или 10 символов.
-   Строка с датой и временем: `YYYYMMDDhhmmss`, `DD/MM/YYYY hh:mm:ss`, `DD-MM-YY hh:mm`, `YYYY-MM-DD hh:mm:ss`, etc.
-   Строка с датой, но без времени: `YYYY`, `YYYYMM`, `YYYY*MM`, `DD/MM/YYYY`, `DD-MM-YY` и т.д.
-   Строка с временем, и с днём: `DD`, `DD hh`, `DD hh:mm`. В этом случае `MM` принимается равным `01`.
-   Строка, содержащая дату и время вместе с информацией о часовом поясе: `YYYY-MM-DD hh:mm:ss ±h:mm`, и т.д. Например, `2020-12-12 17:36:00 -5:00`.
-   Строка, содержащая дату и время в формате [syslog timestamp](https://datatracker.ietf.org/doc/html/rfc3164#section-4.1.2): `Mmm dd hh:mm:ss`. Например, `Jun  9 14:20:32`.

Для всех форматов с разделителями функция распознаёт названия месяцев, выраженных в виде полного англоязычного имени месяца или в виде первых трёх символов имени месяца. Примеры: `24/DEC/18`, `24-Dec-18`, `01-September-2018`.
Если год не указан, вместо него подставляется текущий год. Если в результате получается будущее время (даже на одну секунду впереди текущего момента времени), то текущий год заменяется на прошлый.

**Возвращаемое значение**

-   `time_string` преобразованная к типу данных `DateTime`.

**Примеры**

Запрос:

``` sql
SELECT parseDateTimeBestEffort('23/10/2020 12:12:57')
AS parseDateTimeBestEffort;
```

Результат:

``` text
┌─parseDateTimeBestEffort─┐
│     2020-10-23 12:12:57 │
└─────────────────────────┘
```

Запрос:

``` sql
SELECT parseDateTimeBestEffort('Sat, 18 Aug 2018 07:22:16 GMT', 'Europe/Moscow')
AS parseDateTimeBestEffort;
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
AS parseDateTimeBestEffort;
```

Результат:

``` text
┌─parseDateTimeBestEffort─┐
│     2015-07-07 12:04:41 │
└─────────────────────────┘
```

Запрос:

``` sql
SELECT parseDateTimeBestEffort('2018-10-23 10:12:12')
AS parseDateTimeBestEffort;
```

Результат:

``` text
┌─parseDateTimeBestEffort─┐
│     2018-10-23 10:12:12 │
└─────────────────────────┘
```

Запрос:

``` sql
SELECT toYear(now()) as year, parseDateTimeBestEffort('10 20:19');
```

Результат:

``` text
┌─year─┬─parseDateTimeBestEffort('10 20:19')─┐
│ 2023 │                 2023-01-10 20:19:00 │
└──────┴─────────────────────────────────────┘
```

Запрос:

``` sql
WITH
    now() AS ts_now,
    formatDateTime(ts_around, '%b %e %T') AS syslog_arg
SELECT
    ts_now,
    syslog_arg,
    parseDateTimeBestEffort(syslog_arg)
FROM (SELECT arrayJoin([ts_now - 30, ts_now + 30]) AS ts_around);
```

Результат:

``` text
┌──────────────ts_now─┬─syslog_arg──────┬─parseDateTimeBestEffort(syslog_arg)─┐
│ 2023-06-30 23:59:30 │ Jun 30 23:59:00 │                 2023-06-30 23:59:00 │
│ 2023-06-30 23:59:30 │ Jul  1 00:00:00 │                 2022-07-01 00:00:00 │
└─────────────────────┴─────────────────┴─────────────────────────────────────┘
```

**Смотрите также**

-   [Информация о формате ISO 8601 от @xkcd](https://xkcd.com/1179/)
-   [RFC 1123](https://datatracker.ietf.org/doc/html/rfc1123)
-   [toDate](#todate)
-   [toDateTime](#todatetime)
-   [RFC 3164](https://datatracker.ietf.org/doc/html/rfc3164#section-4.1.2)

## parseDateTimeBestEffortUS {#parsedatetimebesteffortUS}

Эта функция ведет себя как [parseDateTimeBestEffort](#parsedatetimebesteffort) для форматов даты ISO, например, `YYYY-MM-DD hh:mm:ss`, и других форматов даты, где компоненты месяца и дня могут быть однозначно выделены, например, `YYYYMMDDhhmmss`, `YYYY-MM`, `DD hh`, или `YYYY-MM-DD hh:mm:ss ±h:mm`. Если месяц и день не могут быть однозначно выделены, например, `MM/DD/YYY`, `MM-DD-YYYY` или `MM-DD-YY`, то вместо `DD/MM/YYY`, `DD-MM-YYYY` или `DD-MM-YY` предпочитается формат даты США. Впрочем, если номер месяца был бы больше 12 и меньше или равен 31, эта функция возвращается к поведению [parseDateTimeBestEffort](#parsedatetimebesteffort), т.е. `15/08/2020` будет разобрано как `2020-08-15`.

## parseDateTimeBestEffortOrNull {#parsedatetimebesteffortornull}
## parseDateTime32BestEffortOrNull {#parsedatetime32besteffortornull}

Работает также как [parseDateTimeBestEffort](#parsedatetimebesteffort), но возвращает `NULL` когда получает формат даты который не может быть обработан.

## parseDateTimeBestEffortOrZero {#parsedatetimebesteffortorzero}
## parseDateTime32BestEffortOrZero {#parsedatetime32besteffortorzero}

Работает аналогично функции [parseDateTimeBestEffort](#parsedatetimebesteffort), но возвращает нулевое значение, если формат даты не может быть обработан.

## parseDateTimeBestEffortUSOrNull {#parsedatetimebesteffortusornull}

Работает аналогично функции [parseDateTimeBestEffortUS](#parsedatetimebesteffortUS), но в отличие от нее возвращает `NULL`, если входная строка не может быть преобразована в тип данных [DateTime](../../sql-reference/data-types/datetime.md).

## parseDateTimeBestEffortUSOrZero {#parsedatetimebesteffortusorzero}

Работает аналогично функции [parseDateTimeBestEffortUS](#parsedatetimebesteffortUS), но в отличие от нее возвращает нулевую дату (`1970-01-01`) или нулевую дату со временем (`1970-01-01 00:00:00`), если входная строка не может быть преобразована в тип данных [DateTime](../../sql-reference/data-types/datetime.md).

## parseDateTime64BestEffort {#parsedatetime64besteffort}

Работает аналогично функции [parseDateTimeBestEffort](#parsedatetimebesteffort), но также принимает миллисекунды и микросекунды. Возвращает тип данных [DateTime](../../sql-reference/functions/type-conversion-functions.md#data_type-datetime).

**Синтаксис**

``` sql
parseDateTime64BestEffort(time_string [, precision [, time_zone]])
```

**Аргументы**

-   `time_string` — строка, содержащая дату или дату со временем, которые нужно преобразовать. [String](../../sql-reference/data-types/string.md).
-   `precision` — требуемая точность: `3` — для миллисекунд, `6` — для микросекунд. По умолчанию — `3`. Необязательный. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `time_zone` — [Timezone](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone). Разбирает значение `time_string` в зависимости от часового пояса. Необязательный. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   `time_string`, преобразованная в тип данных [DateTime](../../sql-reference/data-types/datetime.md).

**Примеры**

Запрос:

```sql
SELECT parseDateTime64BestEffort('2021-01-01') AS a, toTypeName(a) AS t
UNION ALL
SELECT parseDateTime64BestEffort('2021-01-01 01:01:00.12346') AS a, toTypeName(a) AS t
UNION ALL
SELECT parseDateTime64BestEffort('2021-01-01 01:01:00.12346',6) AS a, toTypeName(a) AS t
UNION ALL
SELECT parseDateTime64BestEffort('2021-01-01 01:01:00.12346',3,'Europe/Moscow') AS a, toTypeName(a) AS t
FORMAT PrettyCompactMonoBlock;
```

Результат:

```
┌──────────────────────────a─┬─t──────────────────────────────┐
│ 2021-01-01 01:01:00.123000 │ DateTime64(3)                  │
│ 2021-01-01 00:00:00.000000 │ DateTime64(3)                  │
│ 2021-01-01 01:01:00.123460 │ DateTime64(6)                  │
│ 2020-12-31 22:01:00.123000 │ DateTime64(3, 'Europe/Moscow') │
└────────────────────────────┴────────────────────────────────┘
```

## parseDateTime64BestEffortUS {#parsedatetime64besteffortus}

Работает аналогично функции [parseDateTime64BestEffort](#parsedatetime64besteffort), но разница состоит в том, что в она предполагает американский формат даты (`MM/DD/YYYY` etc.) в случае неоднозначности.

## parseDateTime64BestEffortOrNull {#parsedatetime64besteffortornull}

Работает аналогично функции [parseDateTime64BestEffort](#parsedatetime64besteffort), но возвращает `NULL`, если формат даты не может быть обработан.

## parseDateTime64BestEffortOrZero {#parsedatetime64besteffortorzero}

Работает аналогично функции [parseDateTime64BestEffort](#parsedatetime64besteffort), но возвращает нулевую дату и время, если формат даты не может быть обработан.

## parseDateTime64BestEffortUSOrNull {#parsedatetime64besteffortusornull}

Работает аналогично функции [parseDateTime64BestEffort](#parsedatetime64besteffort), но разница состоит в том, что в она предполагает американский формат даты (`MM/DD/YYYY` etc.) в случае неоднозначности и возвращает `NULL`, если формат даты не может быть обработан.

## parseDateTime64BestEffortUSOrZero {#parsedatetime64besteffortusorzero}

Работает аналогично функции [parseDateTime64BestEffort](#parsedatetime64besteffort), но разница состоит в том, что в она предполагает американский формат даты (`MM/DD/YYYY` etc.) в случае неоднозначности и возвращает нулевую дату и время, если формат даты не может быть обработан.

## toLowCardinality {#tolowcardinality}

Преобразует входные данные в версию [LowCardinality](../data-types/lowcardinality.md) того же типа данных.

Чтобы преобразовать данные из типа `LowCardinality`, используйте функцию [CAST](#type_conversion_function-cast). Например, `CAST(x as String)`.

**Синтаксис**

```sql
toLowCardinality(expr)
```

**Аргументы**

-   `expr` — [выражение](../syntax.md#syntax-expressions), которое в результате преобразуется в один из [поддерживаемых типов данных](../data-types/index.md#data_types).

**Возвращаемое значение**

-   Результат преобразования `expr`.

Тип: `LowCardinality(expr_result_type)`

**Пример**

Запрос:

```sql
SELECT toLowCardinality('1');
```

Результат:

```text
┌─toLowCardinality('1')─┐
│ 1                     │
└───────────────────────┘
```

## toUnixTimestamp64Milli {#tounixtimestamp64milli}

## toUnixTimestamp64Micro {#tounixtimestamp64micro}

## toUnixTimestamp64Nano {#tounixtimestamp64nano}

Преобразует значение `DateTime64` в значение `Int64` с фиксированной точностью менее одной секунды.
Входное значение округляется соответствующим образом вверх или вниз в зависимости от его точности.

:::info Примечание
Возвращаемое значение — это временная метка в UTC, а не в часовом поясе `DateTime64`.
:::

**Синтаксис**

```sql
toUnixTimestamp64Milli(value)
```

**Аргументы**

-   `value` — значение `DateTime64` с любой точностью.

**Возвращаемое значение**

-   Значение `value`, преобразованное в тип данных `Int64`.

**Примеры**

Запрос:

```sql
WITH toDateTime64('2019-09-16 19:20:12.345678910', 6) AS dt64
SELECT toUnixTimestamp64Milli(dt64);
```

Результат:

``` text
┌─toUnixTimestamp64Milli(dt64)─┐
│                1568650812345 │
└──────────────────────────────┘
```

Запрос:

``` sql
WITH toDateTime64('2019-09-16 19:20:12.345678910', 6) AS dt64
SELECT toUnixTimestamp64Nano(dt64);
```

Результат:

``` text
┌─toUnixTimestamp64Nano(dt64)─┐
│         1568650812345678000 │
└─────────────────────────────┘
```

## fromUnixTimestamp64Milli {#fromunixtimestamp64milli}

## fromUnixTimestamp64Micro {#fromunixtimestamp64micro}

## fromUnixTimestamp64Nano {#fromunixtimestamp64nano}

Преобразует значение `Int64` в значение `DateTime64` с фиксированной точностью менее одной секунды и дополнительным часовым поясом. Входное значение округляется соответствующим образом вверх или вниз в зависимости от его точности. Обратите внимание, что входное значение обрабатывается как метка времени UTC, а не метка времени в заданном (или неявном) часовом поясе.

**Синтаксис**

``` sql
fromUnixTimestamp64Milli(value [, ti])
```

**Аргументы**

-   `value` — значение типы `Int64` с любой точностью.
-   `timezone` — (не обязательный параметр) часовой пояс в формате `String` для возвращаемого результата.

**Возвращаемое значение**

-   Значение `value`, преобразованное в тип данных `DateTime64`.

**Пример**

Запрос:

``` sql
WITH CAST(1234567891011, 'Int64') AS i64
SELECT fromUnixTimestamp64Milli(i64, 'UTC');
```

Результат:

``` text
┌─fromUnixTimestamp64Milli(i64, 'UTC')─┐
│              2009-02-13 23:31:31.011 │
└──────────────────────────────────────┘
```

## formatRow {#formatrow}

Преобразует произвольные выражения в строку заданного формата.

**Синтаксис**

``` sql
formatRow(format, x, y, ...)
```

**Аргументы**

-   `format` — текстовый формат. Например, [CSV](../../interfaces/formats.md#csv), [TSV](../../interfaces/formats.md#tabseparated).
-   `x`,`y`, ... — выражения.

**Возвращаемое значение**

-   Отформатированная строка. (в текстовых форматах обычно с завершающим переводом строки).

**Пример**

Запрос:

``` sql
SELECT formatRow('CSV', number, 'good')
FROM numbers(3);
```

Результат:

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

**Примечание**: если формат содержит префикс/суффикс, то он будет записан в каждой строке.

**Пример**

Запрос:

``` sql
SELECT formatRow('CustomSeparated', number, 'good')
FROM numbers(3)
SETTINGS format_custom_result_before_delimiter='<prefix>\n', format_custom_result_after_delimiter='<suffix>'
```

Результат:

``` text
┌─formatRow('CustomSeparated', number, 'good')─┐
│ <prefix>
0	good
<suffix>                   │
│ <prefix>
1	good
<suffix>                   │
│ <prefix>
2	good
<suffix>                   │
└──────────────────────────────────────────────┘
```

**Примечание**: данная функция поддерживает только строковые форматы вывода.

## formatRowNoNewline {#formatrownonewline}

Преобразует произвольные выражения в строку заданного формата. Отличается от функции formatRow тем, что удаляет лишний перевод строки `\n` а конце, если он есть.

**Синтаксис**

``` sql
formatRowNoNewline(format, x, y, ...)
```

**Аргументы**

-   `format` — текстовый формат. Например, [CSV](../../interfaces/formats.md#csv), [TSV](../../interfaces/formats.md#tabseparated).
-   `x`,`y`, ... — выражения.

**Возвращаемое значение**

-   Отформатированная строка (в текстовых форматах без завершающего перевода строки).

**Пример**

Запрос:

``` sql
SELECT formatRowNoNewline('CSV', number, 'good')
FROM numbers(3);
```

Результат:

``` text
┌─formatRowNoNewline('CSV', number, 'good')─┐
│ 0,"good"                                  │
│ 1,"good"                                  │
│ 2,"good"                                  │
└───────────────────────────────────────────┘
```

## snowflakeToDateTime {#snowflaketodatetime}

Извлекает время из [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) в формате [DateTime](../data-types/datetime.md).

**Синтаксис**

``` sql
snowflakeToDateTime(value [, time_zone])
```

**Аргументы**

-   `value` — Snowflake ID. [Int64](../data-types/int-uint.md).
-   `time_zone` — [временная зона сервера](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone). Функция распознает `time_string` в соответствии с часовым поясом. Необязательный. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-  Значение, преобразованное в фомат [DateTime](../data-types/datetime.md).

**Пример**

Запрос:

``` sql
SELECT snowflakeToDateTime(CAST('1426860702823350272', 'Int64'), 'UTC');
```

Результат:

``` text

┌─snowflakeToDateTime(CAST('1426860702823350272', 'Int64'), 'UTC')─┐
│                                              2021-08-15 10:57:56 │
└──────────────────────────────────────────────────────────────────┘
```

## snowflakeToDateTime64 {#snowflaketodatetime64}

Извлекает время из [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) в формате [DateTime64](../data-types/datetime64.md).

**Синтаксис**

``` sql
snowflakeToDateTime64(value [, time_zone])
```

**Аргументы**

-   `value` — Snowflake ID. [Int64](../data-types/int-uint.md).
-   `time_zone` — [временная зона сервера](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone). Функция распознает `time_string` в соответствии с часовым поясом. Необязательный. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-  Значение, преобразованное в фомат [DateTime64](../data-types/datetime64.md).

**Пример**

Запрос:

``` sql
SELECT snowflakeToDateTime64(CAST('1426860802823350272', 'Int64'), 'UTC');
```

Результат:

``` text

┌─snowflakeToDateTime64(CAST('1426860802823350272', 'Int64'), 'UTC')─┐
│                                            2021-08-15 10:58:19.841 │
└────────────────────────────────────────────────────────────────────┘
```

## dateTimeToSnowflake {#datetimetosnowflake}

Преобразует значение [DateTime](../data-types/datetime.md) в первый идентификатор [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) на текущий момент.

**Syntax**

``` sql
dateTimeToSnowflake(value)
```

**Аргументы**

-   `value` — дата и время. [DateTime](../../sql-reference/data-types/datetime.md).

**Возвращаемое значение**

-   Значение, преобразованное в [Int64](../data-types/int-uint.md), как первый идентификатор Snowflake ID в момент выполнения.

**Пример**

Запрос:

``` sql
WITH toDateTime('2021-08-15 18:57:56', 'Asia/Shanghai') AS dt SELECT dateTimeToSnowflake(dt);
```

Результат:

``` text
┌─dateTimeToSnowflake(dt)─┐
│     1426860702823350272 │
└─────────────────────────┘
```

## dateTime64ToSnowflake {#datetime64tosnowflake}

Преобразует значение [DateTime64](../data-types/datetime64.md) в первый идентификатор [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) на текущий момент.

**Синтаксис**

``` sql
dateTime64ToSnowflake(value)
```

**Аргументы**

-   `value` — дата и время. [DateTime64](../data-types/datetime64.md).

**Возвращаемое значение**

-   Значение, преобразованное в [Int64](../data-types/int-uint.md), как первый идентификатор Snowflake ID в момент выполнения.


**Пример**

Запрос:

``` sql
WITH toDateTime64('2021-08-15 18:57:56.492', 3, 'Asia/Shanghai') AS dt64 SELECT dateTime64ToSnowflake(dt64);
```

Результат:

``` text
┌─dateTime64ToSnowflake(dt64)─┐
│         1426860704886947840 │
└─────────────────────────────┘
```
