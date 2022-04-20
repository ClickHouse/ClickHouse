---
sidebar_position: 52
sidebar_label: "Функции кодирования"
---

# Функции кодирования {#funktsii-kodirovaniia}

## char {#char}

Возвращает строку, длина которой равна числу переданных аргументов, и каждый байт имеет значение соответствующего аргумента. Принимает несколько числовых аргументов. Если значение аргумента выходит за диапазон UInt8 (0..255), то оно преобразуется в UInt8 с возможным округлением и переполнением.

**Синтаксис**

``` sql
char(number_1, [number_2, ..., number_n]);
```

**Аргументы**

-   `number_1, number_2, ..., number_n` — числовые аргументы, которые интерпретируются как целые числа. Типы: [Int](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md).

**Возвращаемое значение**

-   Строка из соответствующих байт.

Тип: [String](../../sql-reference/data-types/string.md).

**Пример**

Запрос:

``` sql
SELECT char(104.1, 101, 108.9, 108.9, 111) AS hello;
```

Результат:

``` text
┌─hello─┐
│ hello │
└───────┘
```

Вы можете создать строку в произвольной кодировке, передав соответствующие байты. Пример для UTF-8:

Запрос:

``` sql
SELECT char(0xD0, 0xBF, 0xD1, 0x80, 0xD0, 0xB8, 0xD0, 0xB2, 0xD0, 0xB5, 0xD1, 0x82) AS hello;
```

Результат:

``` text
┌─hello──┐
│ привет │
└────────┘
```

Запрос:

``` sql
SELECT char(0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD) AS hello;
```

Результат:

``` text
┌─hello─┐
│ 你好  │
└───────┘
```

## hex {#hex}

Возвращает строку, содержащую шестнадцатеричное представление аргумента.

Синоним: `HEX`.

**Синтаксис**

``` sql
hex(arg)
```

Функция использует прописные буквы `A-F` и не использует никаких префиксов (например, `0x`) или суффиксов (например, `h`).

Для целочисленных аргументов возвращает шестнадцатеричные цифры от наиболее до наименее значимых (`big endian`, человекочитаемый порядок).Он начинается с самого значимого ненулевого байта (начальные нулевые байты опущены), но всегда выводит обе цифры каждого байта, даже если начальная цифра равна нулю.

Значения типа [Date](../../sql-reference/data-types/date.md) и [DateTime](../../sql-reference/data-types/datetime.md) формируются как соответствующие целые числа (количество дней с момента Unix-эпохи для `Date` и значение Unix Timestamp для `DateTime`).

Для [String](../../sql-reference/data-types/string.md) и [FixedString](../../sql-reference/data-types/fixedstring.md), все байты просто кодируются как два шестнадцатеричных числа. Нулевые байты не опущены.

Значения [Float](../../sql-reference/data-types/float.md) и [Decimal](../../sql-reference/data-types/decimal.md) кодируются как их представление в памяти. Поскольку ClickHouse поддерживает архитектуру `little-endian`, они кодируются от младшего к старшему байту. Нулевые начальные/конечные байты не опущены.

**Аргументы**

-   `arg` — значение для преобразования в шестнадцатеричное. [String](../../sql-reference/data-types/string.md), [UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md), [Decimal](../../sql-reference/data-types/decimal.md), [Date](../../sql-reference/data-types/date.md) или [DateTime](../../sql-reference/data-types/datetime.md).

**Возвращаемое значение**

-   Строка — шестнадцатеричное представление аргумента.

Тип: [String](../../sql-reference/data-types/string.md).

**Примеры**

Запрос:

``` sql
SELECT hex(1);
```

Результат:

``` text
01
```

Запрос:

``` sql
SELECT hex(toFloat32(number)) AS hex_presentation FROM numbers(15, 2);
```

Результат:

``` text
┌─hex_presentation─┐
│ 00007041         │
│ 00008041         │
└──────────────────┘
```

Запрос:

``` sql
SELECT hex(toFloat64(number)) AS hex_presentation FROM numbers(15, 2);
```

Результат:

``` text
┌─hex_presentation─┐
│ 0000000000002E40 │
│ 0000000000003040 │
└──────────────────┘
```

## unhex(str) {#unhexstr}

Выполняет операцию, обратную [hex](#hex). Функция интерпретирует каждую пару шестнадцатеричных цифр аргумента как число и преобразует его в символ. Возвращаемое значение представляет собой двоичную строку (BLOB).

Если вы хотите преобразовать результат в число, вы можете использовать функции [reverse](../../sql-reference/functions/string-functions.md#reverse) и [reinterpretAs&lt;Type&gt;](../../sql-reference/functions/type-conversion-functions.md#type-conversion-functions).

    :::note "Примечание"
    Если `unhex` вызывается из `clickhouse-client`, двоичные строки отображаются с использованием UTF-8.
    :::
Синоним: `UNHEX`.

**Синтаксис**

``` sql
unhex(arg)
```

**Аргументы**

-   `arg` — Строка, содержащая любое количество шестнадцатеричных цифр. Тип: [String](../../sql-reference/data-types/string.md).

Поддерживаются как прописные, так и строчные буквы `A-F`. Количество шестнадцатеричных цифр не обязательно должно быть четным. Если оно нечетное, последняя цифра интерпретируется как наименее значимая половина байта `00-0F`. Если строка аргумента содержит что-либо, кроме шестнадцатеричных цифр, возвращается некоторый результат, определенный реализацией (исключение не создается).

**Возвращаемое значение**

-   Бинарная строка (BLOB).

Тип: [String](../../sql-reference/data-types/string.md).

**Пример**

Запрос:
``` sql
SELECT unhex('303132'), UNHEX('4D7953514C');
```

Результат:
``` text
┌─unhex('303132')─┬─unhex('4D7953514C')─┐
│ 012             │ MySQL               │
└─────────────────┴─────────────────────┘
```

Запрос:

``` sql
SELECT reinterpretAsUInt64(reverse(unhex('FFF'))) AS num;
```

Результат:

``` text
┌──num─┐
│ 4095 │
└──────┘
```

## bin {#bin}

Возвращает строку, содержащую бинарное представление аргумента.

**Синтаксис**

``` sql
bin(arg)
```

Синоним: `BIN`.

Для целочисленных аргументов возвращаются двоичные числа от наиболее значимого до наименее значимого (`big-endian`, человекочитаемый порядок). Порядок начинается с самого значимого ненулевого байта (начальные нулевые байты опущены), но всегда возвращает восемь цифр каждого байта, если начальная цифра равна нулю.

Значения типа [Date](../../sql-reference/data-types/date.md) и [DateTime](../../sql-reference/data-types/datetime.md) формируются как соответствующие целые числа (количество дней с момента Unix-эпохи для `Date` и значение Unix Timestamp для `DateTime`).

Для [String](../../sql-reference/data-types/string.md) и [FixedString](../../sql-reference/data-types/fixedstring.md) все байты кодируются как восемь двоичных чисел. Нулевые байты не опущены.

Значения [Float](../../sql-reference/data-types/float.md) и [Decimal](../../sql-reference/data-types/decimal.md) кодируются как их представление в памяти. Поскольку ClickHouse поддерживает архитектуру `little-endian`, они кодируются от младшего к старшему байту. Нулевые начальные/конечные байты не опущены.

**Аргументы**

-   `arg` — значение для преобразования в двоичный код. [String](../../sql-reference/data-types/string.md), [FixedString](../../sql-reference/data-types/fixedstring.md), [UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md), [Decimal](../../sql-reference/data-types/decimal.md), [Date](../../sql-reference/data-types/date.md) или [DateTime](../../sql-reference/data-types/datetime.md).

**Возвращаемое значение**

-   Бинарная строка (BLOB) — двоичное представление аргумента.

Тип: [String](../../sql-reference/data-types/string.md).

**Примеры**

Запрос:

``` sql
SELECT bin(14);
```

Результат:

``` text
┌─bin(14)──┐
│ 00001110 │
└──────────┘
```

Запрос:

``` sql
SELECT bin(toFloat32(number)) AS bin_presentation FROM numbers(15, 2);
```

Результат:

``` text
┌─bin_presentation─────────────────┐
│ 00000000000000000111000001000001 │
│ 00000000000000001000000001000001 │
└──────────────────────────────────┘
```

Запрос:

``` sql
SELECT bin(toFloat64(number)) AS bin_presentation FROM numbers(15, 2);
```

Результат:

``` text
┌─bin_presentation─────────────────────────────────────────────────┐
│ 0000000000000000000000000000000000000000000000000010111001000000 │
│ 0000000000000000000000000000000000000000000000000011000001000000 │
└──────────────────────────────────────────────────────────────────┘
```

## unbin {#unbinstr}

Интерпретирует каждую пару двоичных цифр аргумента как число и преобразует его в байт, представленный числом. Функция выполняет операцию, противоположную [bin](#bin).

**Синтаксис**

``` sql
unbin(arg)
```

Синоним: `UNBIN`.

Для числового аргумента `unbin()` не возвращает значение, обратное результату `bin()`. Чтобы преобразовать результат в число, используйте функции [reverse](../../sql-reference/functions/string-functions.md#reverse) и [reinterpretAs&lt;Type&gt;](../../sql-reference/functions/type-conversion-functions.md#reinterpretasuint8163264).

    :::note "Примечание"
    Если `unbin` вызывается из клиента `clickhouse-client`, бинарная строка возвращается в кодировке UTF-8.
    :::
Поддерживает двоичные цифры `0` и `1`. Количество двоичных цифр не обязательно должно быть кратно восьми. Если строка аргумента содержит что-либо, кроме двоичных цифр, возвращается некоторый результат, определенный реализацией (ошибки не возникает). 

**Аргументы**

-   `arg` — строка, содержащая любое количество двоичных цифр. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   Бинарная строка (BLOB).

Тип: [String](../../sql-reference/data-types/string.md).

**Примеры**

Запрос:

``` sql
SELECT UNBIN('001100000011000100110010'), UNBIN('0100110101111001010100110101000101001100');
```

Результат:

``` text
┌─unbin('001100000011000100110010')─┬─unbin('0100110101111001010100110101000101001100')─┐
│ 012                               │ MySQL                                             │
└───────────────────────────────────┴───────────────────────────────────────────────────┘
```

Запрос:

``` sql
SELECT reinterpretAsUInt64(reverse(unbin('1110'))) AS num;
```

Результат:

``` text
┌─num─┐
│  14 │
└─────┘
```

## UUIDStringToNum(str) {#uuidstringtonumstr}

Принимает строку, содержащую 36 символов в формате `123e4567-e89b-12d3-a456-426655440000`, и возвращает в виде набора байт в FixedString(16).

## UUIDNumToString(str) {#uuidnumtostringstr}

Принимает значение типа FixedString(16). Возвращает строку из 36 символов в текстовом виде.

## bitmaskToList(num) {#bitmasktolistnum}

Принимает целое число. Возвращает строку, содержащую список степеней двойки, в сумме дающих исходное число; по возрастанию, в текстовом виде, через запятую, без пробелов.

## bitmaskToArray(num) {#bitmasktoarraynum}

Принимает целое число. Возвращает массив чисел типа UInt64, содержащий степени двойки, в сумме дающих исходное число; числа в массиве идут по возрастанию.

## bitPositionsToArray(num) {#bitpositionstoarraynum}

Принимает целое число и приводит его к беззнаковому виду. Возвращает массив `UInt64` чисел, который содержит список позиций битов `arg`, равных `1`, в порядке возрастания.

**Синтаксис**

```sql
bitPositionsToArray(arg)
```

**Аргументы**

-   `arg` — целое значение. [Int/UInt](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Массив, содержащий список позиций битов, равных `1`, в порядке возрастания.

Тип: [Array](../../sql-reference/data-types/array.md)([UInt64](../../sql-reference/data-types/int-uint.md)).

**Примеры**

Запрос:

``` sql
SELECT bitPositionsToArray(toInt8(1)) AS bit_positions;
```

Результат:

``` text
┌─bit_positions─┐
│ [0]           │
└───────────────┘
```

Запрос:

``` sql
SELECT bitPositionsToArray(toInt8(-1)) AS bit_positions;
```

Результат:

``` text
┌─bit_positions─────┐
│ [0,1,2,3,4,5,6,7] │
└───────────────────┘
```
