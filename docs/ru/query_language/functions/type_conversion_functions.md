# Функции преобразования типов

## toUInt8, toUInt16, toUInt32, toUInt64

## toInt8, toInt16, toInt32, toInt64

## toFloat32, toFloat64

## toDate, toDateTime

## toUInt8OrZero, toUInt16OrZero, toUInt32OrZero, toUInt64OrZero, toInt8OrZero, toInt16OrZero, toInt32OrZero, toInt64OrZero, toFloat32OrZero, toFloat64OrZero

## toUInt8OrNull, toUInt16OrNull, toUInt32OrNull, toUInt64OrNull, toInt8OrNull, toInt16OrNull, toInt32OrNull, toInt64OrNull, toFloat32OrNull, toFloat64OrNull, toDateOrNull, toDateTimeOrNull

## toDecimal32(value, S), toDecimal64(value, S), toDecimal128(value, S)

Преобразует тип `value` в тип [Decimal](../../data_types/decimal.md), имеющий точность `S`. `value` может быть числом или строкой. Параметр `S` (scale) устанавливает количество десятичных знаков.

## toDecimal(32|64|128)OrNull

Преобразует входную строку в значение с типом данных [Nullable (Decimal (P, S))](../../data_types/decimal.md). Семейство функций включает в себя:

- `toDecimal32OrNull(expr, S)` — Возвращает значение типа `Nullable(Decimal32(S))`.
- `toDecimal64OrNull(expr, S)` — Возвращает значение типа `Nullable(Decimal64(S))`.
- `toDecimal128OrNull(expr, S)` — Возвращает значение типа `Nullable(Decimal128(S))`.

Эти функции следует использовать вместо функций `toDecimal*()`, если при ошибке обработки входного значения вы хотите получать `NULL` вместо исключения.

**Параметры**

- `expr` — [выражение](../syntax.md#syntax-expressions), возвращающее значение типа [String](../../data_types/string.md). ClickHouse ожидает текстовое представление десятичного числа. Например, `'1.111'`.
- `S` — количество десятичных знаков в результирующем значении.

**Возвращаемое значение**

Значение типа `Nullable(Decimal(P,S))`. Значение содержит:

- Число с `S` десятичными знаками, если ClickHouse распознал число во входной строке.
- `NULL`, если ClickHouse не смог распознать число во входной строке или входное число содержит больше чем `S` десятичных знаков.

**Примеры**

```sql
SELECT toDecimal32OrNull(toString(-1.111), 5) AS val, toTypeName(val)
```

```text
┌──────val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 5))─┐
│ -1.11100 │ Nullable(Decimal(9, 5))                            │
└──────────┴────────────────────────────────────────────────────┘
```

```sql
SELECT toDecimal32OrNull(toString(-1.111), 2) AS val, toTypeName(val)
```

```text
┌──val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 2))─┐
│ ᴺᵁᴸᴸ │ Nullable(Decimal(9, 2))                            │
└──────┴────────────────────────────────────────────────────┘
```

## toDecimal(32|64|128)OrZero

Преобразует тип входного значения в [Decimal (P, S)](../../data_types/decimal.md). Семейство функций включает в себя:

- `toDecimal32OrZero( expr, S)` — возвращает значение типа `Decimal32(S)`.
- `toDecimal64OrZero( expr, S)` — возвращает значение типа `Decimal64(S)`.
- `toDecimal128OrZero( expr, S)` — возвращает значение типа `Decimal128(S)`.

Эти функции следует использовать вместо функций `toDecimal*()`, если при ошибке обработки входного значения вы хотите получать `0` вместо исключения.

**Параметры**

- `expr` — [выражение](../syntax.md#syntax-expressions), возвращающее значение типа [String](../../data_types/string.md). ClickHouse ожидает текстовое представление десятичного числа. Например, `'1.111'`.
- `S` — количество десятичных знаков в результирующем значении.

**Возвращаемое значение**

Значение типа `Nullable(Decimal(P,S))`. `P` равно числовой части имени функции. Например, для функции `toDecimal32OrZero`, `P = 32`. Значение содержит:

- Число с `S` десятичными знаками, если ClickHouse распознал число во входной строке.
- 0 c `S` десятичными знаками, если ClickHouse не смог распознать число во входной строке или входное число содержит больше чем `S` десятичных знаков.

**Пример**

```sql
SELECT toDecimal32OrZero(toString(-1.111), 5) AS val, toTypeName(val)
```

```text
┌──────val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 5))─┐
│ -1.11100 │ Decimal(9, 5)                                      │
└──────────┴────────────────────────────────────────────────────┘
```

```sql
SELECT toDecimal32OrZero(toString(-1.111), 2) AS val, toTypeName(val)
```

```text
┌──val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 2))─┐
│ 0.00 │ Decimal(9, 2)                                      │
└──────┴────────────────────────────────────────────────────┘
```

## toString

Функции преобразования между числами, строками (но не фиксированными строками), датами и датами-с-временем.
Все эти функции принимают один аргумент.

При преобразовании в строку или из строки, производится форматирование или парсинг значения по тем же правилам, что и для формата TabSeparated (и почти всех остальных текстовых форматов). Если распарсить строку не удаётся - кидается исключение и выполнение запроса прерывается.

При преобразовании даты в число или наоборот, дате соответствует число дней от начала unix эпохи.
При преобразовании даты-с-временем в число или наоборот, дате-с-временем соответствует число секунд от начала unix эпохи.

Форматы даты и даты-с-временем для функций toDate/toDateTime определены следующим образом:

```
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

В качестве исключения, если делается преобразование из числа типа UInt32, Int32, UInt64, Int64 в Date, и если число больше или равно 65536, то число рассматривается как unix timestamp (а не как число дней) и округляется до даты. Это позволяет поддержать распространённый случай, когда пишут toDate(unix_timestamp), что иначе было бы ошибкой и требовало бы написания более громоздкого toDate(toDateTime(unix_timestamp))

Преобразование между датой и датой-с-временем производится естественным образом: добавлением нулевого времени или отбрасыванием времени.

Преобразование между числовыми типами производится по тем же правилам, что и присваивание между разными числовыми типами в C++.

Дополнительно, функция toString от аргумента типа DateTime может принимать второй аргумент String - имя тайм-зоны. Пример: `Asia/Yekaterinburg` В этом случае, форматирование времени производится согласно указанной тайм-зоне.

```sql
SELECT
    now() AS now_local,
    toString(now(), 'Asia/Yekaterinburg') AS now_yekat
```

```
┌───────────now_local─┬─now_yekat───────────┐
│ 2016-06-15 00:11:21 │ 2016-06-15 02:11:21 │
└─────────────────────┴─────────────────────┘
```

Также смотрите функцию `toUnixTimestamp`.

## toFixedString(s, N)

Преобразует аргумент типа String в тип FixedString(N) (строку фиксированной длины N). N должно быть константой.
Если строка имеет меньше байт, чем N, то она дополняется нулевыми байтами справа. Если строка имеет больше байт, чем N - кидается исключение.

## toStringCutToZero(s)

Принимает аргумент типа String или FixedString. Возвращает String, вырезая содержимое строки до первого найденного нулевого байта.

Пример:

```sql
SELECT toFixedString('foo', 8) AS s, toStringCutToZero(s) AS s_cut
```

```
┌─s─────────────┬─s_cut─┐
│ foo\0\0\0\0\0 │ foo   │
└───────────────┴───────┘
```

```sql
SELECT toFixedString('foo\0bar', 8) AS s, toStringCutToZero(s) AS s_cut
```

```
┌─s──────────┬─s_cut─┐
│ foo\0bar\0 │ foo   │
└────────────┴───────┘
```

## reinterpretAsUInt8, reinterpretAsUInt16, reinterpretAsUInt32, reinterpretAsUInt64

## reinterpretAsInt8, reinterpretAsInt16, reinterpretAsInt32, reinterpretAsInt64

## reinterpretAsFloat32, reinterpretAsFloat64

## reinterpretAsDate, reinterpretAsDateTime

Функции принимают строку и интерпретируют байты, расположенные в начале строки, как число в host order (little endian). Если строка имеет недостаточную длину, то функции работают так, как будто строка дополнена необходимым количеством нулевых байт. Если строка длиннее, чем нужно, то лишние байты игнорируются. Дата интерпретируется, как число дней с начала unix-эпохи, а дата-с-временем - как число секунд с начала unix-эпохи.

## reinterpretAsString {#type_conversion_functions-reinterpretAsString}
Функция принимает число или дату или дату-с-временем и возвращает строку, содержащую байты, представляющие соответствующее значение в host order (little endian). При этом, отбрасываются нулевые байты с конца. Например, значение 255 типа UInt32 будет строкой длины 1 байт.

## CAST(x, t) {#type_conversion_function-cast}
Преобразует x в тип данных t.
Поддерживается также синтаксис CAST(x AS t).

Пример:

```sql
SELECT
    '2016-06-15 23:00:00' AS timestamp,
    CAST(timestamp AS DateTime) AS datetime,
    CAST(timestamp AS Date) AS date,
    CAST(timestamp, 'String') AS string,
    CAST(timestamp, 'FixedString(22)') AS fixed_string
```

```
┌─timestamp───────────┬────────────datetime─┬───────date─┬─string──────────────┬─fixed_string──────────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00\0\0\0 │
└─────────────────────┴─────────────────────┴────────────┴─────────────────────┴───────────────────────────┘
```

Преобразование в FixedString(N) работает только для аргументов типа String или FixedString(N).

Поддержано преобразование к типу [Nullable](../../data_types/nullable.md) и обратно. Пример:

```
SELECT toTypeName(x) FROM t_null

┌─toTypeName(x)─┐
│ Int8          │
│ Int8          │
└───────────────┘

SELECT toTypeName(CAST(x, 'Nullable(UInt16)')) FROM t_null

┌─toTypeName(CAST(x, 'Nullable(UInt16)'))─┐
│ Nullable(UInt16)                        │
│ Nullable(UInt16)                        │
└─────────────────────────────────────────┘
```

[Оригинальная статья](https://clickhouse.yandex/docs/ru/query_language/functions/type_conversion_functions/) <!--hide-->
