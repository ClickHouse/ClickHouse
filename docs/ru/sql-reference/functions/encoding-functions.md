---
toc_priority: 52
toc_title: "Функции кодирования"
---

# Функции кодирования {#funktsii-kodirovaniia}

## char {#char}

Возвращает строку, длина которой равна числу переданных аргументов, и каждый байт имеет значение соответствующего аргумента. Принимает несколько числовых аргументов. Если значение аргумента выходит за диапазон UInt8 (0..255), то оно преобразуется в UInt8 с возможным округлением и переполнением.

**Синтаксис**

``` sql
char(number_1, [number_2, ..., number_n]);
```

**Аргументы**

-   `number_1, number_2, ..., number_n` — числовые аргументы, которые интерпретируются как целые числа. Типы: [Int](../../sql-reference/functions/encoding-functions.md), [Float](../../sql-reference/functions/encoding-functions.md).

**Возвращаемое значение**

-   Строка из соответствующих байт.

Тип: `String`.

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

Returns a string containing the argument’s hexadecimal representation.

Синоним: `HEX`.

**Syntax**

``` sql
hex(arg)
```

The function is using uppercase letters `A-F` and not using any prefixes (like `0x`) or suffixes (like `h`).

For integer arguments, it prints hex digits («nibbles») from the most significant to least significant (big endian or «human readable» order). It starts with the most significant non-zero byte (leading zero bytes are omitted) but always prints both digits of every byte even if leading digit is zero.

Example:

**Example**

Query:

``` sql
SELECT hex(1);
```

Result:

``` text
01
```

Values of type `Date` and `DateTime` are formatted as corresponding integers (the number of days since Epoch for Date and the value of Unix Timestamp for DateTime).

For `String` and `FixedString`, all bytes are simply encoded as two hexadecimal numbers. Zero bytes are not omitted.

Values of floating point and Decimal types are encoded as their representation in memory. As we support little endian architecture, they are encoded in little endian. Zero leading/trailing bytes are not omitted.

**Parameters**

-   `arg` — A value to convert to hexadecimal. Types: [String](../../sql-reference/functions/encoding-functions.md), [UInt](../../sql-reference/functions/encoding-functions.md), [Float](../../sql-reference/functions/encoding-functions.md), [Decimal](../../sql-reference/functions/encoding-functions.md), [Date](../../sql-reference/functions/encoding-functions.md) or [DateTime](../../sql-reference/functions/encoding-functions.md).

**Returned value**

-   A string with the hexadecimal representation of the argument.

Type: `String`.

**Example**

Query:

``` sql
SELECT hex(toFloat32(number)) as hex_presentation FROM numbers(15, 2);
```

Result:

``` text
┌─hex_presentation─┐
│ 00007041         │
│ 00008041         │
└──────────────────┘
```

Query:

``` sql
SELECT hex(toFloat64(number)) as hex_presentation FROM numbers(15, 2);
```

Result:

``` text
┌─hex_presentation─┐
│ 0000000000002E40 │
│ 0000000000003040 │
└──────────────────┘
```

## unhex(str) {#unhexstr}

Выполняет операцию, обратную [hex](#hex). Функция интерпретирует каждую пару шестнадцатеричных цифр аргумента как число и преобразует его в символ. Возвращаемое значение представляет собой двоичную строку (BLOB).

Если вы хотите преобразовать результат в число, вы можете использовать функции [reverse](../../sql-reference/functions/string-functions.md#reverse) и [reinterpretAs<Type>](../../sql-reference/functions/type-conversion-functions.md#type-conversion-functions).

!!! note "Примечание"
    Если `unhex` вызывается из `clickhouse-client`, двоичные строки отображаются с использованием UTF-8.

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
select bitPositionsToArray(toInt8(-1)) as bit_positions;
```

Результат:

``` text
┌─bit_positions─────┐
│ [0,1,2,3,4,5,6,7] │
└───────────────────┘
```
