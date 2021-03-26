# Функции кодирования {#funktsii-kodirovaniia}

## char {#char}

Возвращает строку, длина которой равна числу переданных аргументов, и каждый байт имеет значение соответствующего аргумента. Принимает несколько числовых аргументов. Если значение аргумента выходит за диапазон UInt8 (0..255), то оно преобразуется в UInt8 с возможным округлением и переполнением.

**Синтаксис**

``` sql
char(number_1, [number_2, ..., number_n]);
```

**Параметры**

-   `number_1, number_2, ..., number_n` — Числовые аргументы, которые интерпретируются как целые числа. Типы: [Int](../../sql-reference/functions/encoding-functions.md), [Float](../../sql-reference/functions/encoding-functions.md).

**Возвращаемое значение**

-   строка из соответствующих байт.

Тип: `String`.

**Пример**

Запрос:

``` sql
SELECT char(104.1, 101, 108.9, 108.9, 111) AS hello
```

Ответ:

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

Ответ:

``` text
┌─hello──┐
│ привет │
└────────┘
```

Запрос:

``` sql
SELECT char(0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD) AS hello;
```

Ответ:

``` text
┌─hello─┐
│ 你好  │
└───────┘
```

## hex {#hex}

Returns a string containing the argument’s hexadecimal representation.

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

Accepts a string containing any number of hexadecimal digits, and returns a string containing the corresponding bytes. Supports both uppercase and lowercase letters A-F. The number of hexadecimal digits does not have to be even. If it is odd, the last digit is interpreted as the least significant half of the 00-0F byte. If the argument string contains anything other than hexadecimal digits, some implementation-defined result is returned (an exception isn’t thrown).
If you want to convert the result to a number, you can use the ‘reverse’ and ‘reinterpretAsType’ functions.

## UUIDStringToNum(str) {#uuidstringtonumstr}

Принимает строку, содержащую 36 символов в формате `123e4567-e89b-12d3-a456-426655440000`, и возвращает в виде набора байт в FixedString(16).

## UUIDNumToString(str) {#uuidnumtostringstr}

Принимает значение типа FixedString(16). Возвращает строку из 36 символов в текстовом виде.

## bitmaskToList(num) {#bitmasktolistnum}

Принимает целое число. Возвращает строку, содержащую список степеней двойки, в сумме дающих исходное число; по возрастанию, в текстовом виде, через запятую, без пробелов.

## bitmaskToArray(num) {#bitmasktoarraynum}

Принимает целое число. Возвращает массив чисел типа UInt64, содержащий степени двойки, в сумме дающих исходное число; числа в массиве идут по возрастанию.

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/functions/encoding_functions/) <!--hide-->
