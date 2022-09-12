---
sidebar_position: 48
sidebar_label: "Битовые функции"
---

# Битовые функции {#bitovye-funktsii}

Битовые функции работают для любой пары типов из `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32`, `Int64`, `Float32`, `Float64`.

Тип результата - целое число, битность которого равна максимальной битности аргументов. Если хотя бы один аргумент знаковый, то результат - знаковое число. Если аргумент - число с плавающей запятой - оно приводится к Int64.

## bitAnd(a, b) {#bitanda-b}

## bitOr(a, b) {#bitora-b}

## bitXor(a, b) {#bitxora-b}

## bitNot(a) {#bitnota}

## bitShiftLeft(a, b) {#bitshiftlefta-b}

Сдвигает влево на заданное количество битов бинарное представление значения.

Если передан аргумент типа `FixedString` или `String`, то он рассматривается, как одно многобайтовое значение. 

Биты `FixedString` теряются по мере того, как выдвигаются за пределы строки. Значение `String` дополняется байтами, поэтому его биты не теряются.

**Синтаксис**

``` sql
bitShiftLeft(a, b)
```

**Аргументы**

-   `a` — сдвигаемое значение. [Целое число](../../sql-reference/data-types/int-uint.md), [String](../../sql-reference/data-types/string.md) или [FixedString](../../sql-reference/data-types/fixedstring.md).
-   `b` — величина сдвига. [Беззнаковое целое число](../../sql-reference/data-types/int-uint.md), допустимы типы с разрядностью не более 64 битов.

**Возвращаемое значение**

-   Сдвинутое значение.

Тип совпадает с типом сдвигаемого значения.

**Пример**

В запросах используются функции [bin](encoding-functions.md#bin) и [hex](encoding-functions.md#hex), чтобы наглядно показать биты после сдвига.

``` sql
SELECT 99 AS a, bin(a), bitShiftLeft(a, 2) AS a_shifted, bin(a_shifted);
SELECT 'abc' AS a, hex(a), bitShiftLeft(a, 4) AS a_shifted, hex(a_shifted);
SELECT toFixedString('abc', 3) AS a, hex(a), bitShiftLeft(a, 4) AS a_shifted, hex(a_shifted);
```

Результат:

``` text
┌──a─┬─bin(99)──┬─a_shifted─┬─bin(bitShiftLeft(99, 2))─┐
│ 99 │ 01100011 │       140 │ 10001100                 │
└────┴──────────┴───────────┴──────────────────────────┘
┌─a───┬─hex('abc')─┬─a_shifted─┬─hex(bitShiftLeft('abc', 4))─┐
│ abc │ 616263     │ &0        │ 06162630                    │
└─────┴────────────┴───────────┴─────────────────────────────┘
┌─a───┬─hex(toFixedString('abc', 3))─┬─a_shifted─┬─hex(bitShiftLeft(toFixedString('abc', 3), 4))─┐
│ abc │ 616263                       │ &0        │ 162630                                        │
└─────┴──────────────────────────────┴───────────┴───────────────────────────────────────────────┘
```

## bitShiftRight(a, b) {#bitshiftrighta-b}

Сдвигает вправо на заданное количество битов бинарное представление значения.

Если передан аргумент типа `FixedString` или `String`, то он рассматривается, как одно многобайтовое значение. Длина значения типа `String` уменьшается по мере сдвига.

**Синтаксис**

``` sql
bitShiftRight(a, b)
```

**Аргументы**

-   `a` — сдвигаемое значение. [Целое число](../../sql-reference/data-types/int-uint.md), [String](../../sql-reference/data-types/string.md) или [FixedString](../../sql-reference/data-types/fixedstring.md).
-   `b` — величина сдвига. [Беззнаковое целое число](../../sql-reference/data-types/int-uint.md), допустимы типы с разрядностью не более 64 битов.

**Возвращаемое значение**

-   Сдвинутое значение.

Тип совпадает с типом сдвигаемого значения.

**Пример**

Запрос:

``` sql
SELECT 101 AS a, bin(a), bitShiftRight(a, 2) AS a_shifted, bin(a_shifted);
SELECT 'abc' AS a, hex(a), bitShiftRight(a, 12) AS a_shifted, hex(a_shifted);
SELECT toFixedString('abc', 3) AS a, hex(a), bitShiftRight(a, 12) AS a_shifted, hex(a_shifted);
```

Результат:

``` text
┌───a─┬─bin(101)─┬─a_shifted─┬─bin(bitShiftRight(101, 2))─┐
│ 101 │ 01100101 │        25 │ 00011001                   │
└─────┴──────────┴───────────┴────────────────────────────┘
┌─a───┬─hex('abc')─┬─a_shifted─┬─hex(bitShiftRight('abc', 12))─┐
│ abc │ 616263     │           │ 0616                          │
└─────┴────────────┴───────────┴───────────────────────────────┘
┌─a───┬─hex(toFixedString('abc', 3))─┬─a_shifted─┬─hex(bitShiftRight(toFixedString('abc', 3), 12))─┐
│ abc │ 616263                       │           │ 000616                                          │
└─────┴──────────────────────────────┴───────────┴─────────────────────────────────────────────────┘
```

## bitTest {#bittest}

Принимает любое целое число и конвертирует его в [двоичное число](https://en.wikipedia.org/wiki/Binary_number), возвращает значение бита в указанной позиции. Отсчет начинается с 0 справа налево.

**Синтаксис**

``` sql
SELECT bitTest(number, index)
```

**Аргументы**

-   `number` – целое число.
-   `index` – позиция бита.

**Возвращаемое значение**

Возвращает значение бита в указанной позиции.

Тип: `UInt8`.

**Пример**

Например, число 43 в двоичной системе счисления равно: 101011.

Запрос:

``` sql
SELECT bitTest(43, 1);
```

Результат:

``` text
┌─bitTest(43, 1)─┐
│              1 │
└────────────────┘
```

Другой пример:

Запрос:

``` sql
SELECT bitTest(43, 2);
```

Результат:

``` text
┌─bitTest(43, 2)─┐
│              0 │
└────────────────┘
```

## bitTestAll {#bittestall}

Возвращает результат [логической конъюнкции](https://en.wikipedia.org/wiki/Logical_conjunction) (оператор AND) всех битов в указанных позициях. Отсчет начинается с 0 справа налево.

Бинарная конъюнкция:

0 AND 0 = 0
0 AND 1 = 0
1 AND 0 = 0
1 AND 1 = 1

**Синтаксис**

``` sql
SELECT bitTestAll(number, index1, index2, index3, index4, ...)
```

**Аргументы**

-   `number` – целое число.
-   `index1`, `index2`, `index3`, `index4` – позиция бита. Например, конъюнкция для набора позиций `index1`, `index2`, `index3`, `index4` является истинной, если все его позиции истинны `index1` ⋀ `index2` ⋀ `index3` ⋀ `index4`.

**Возвращаемое значение**

Возвращает результат логической конъюнкции.

Тип: `UInt8`.

**Пример**

Например, число 43 в двоичной системе счисления равно: 101011.

Запрос:

``` sql
SELECT bitTestAll(43, 0, 1, 3, 5);
```

Результат:

``` text
┌─bitTestAll(43, 0, 1, 3, 5)─┐
│                          1 │
└────────────────────────────┘
```

Другой пример:

Запрос:

``` sql
SELECT bitTestAll(43, 0, 1, 3, 5, 2);
```

Результат:

``` text
┌─bitTestAll(43, 0, 1, 3, 5, 2)─┐
│                             0 │
└───────────────────────────────┘
```

## bitTestAny {#bittestany}

Возвращает результат [логической дизъюнкции](https://en.wikipedia.org/wiki/Logical_disjunction) (оператор OR) всех битов в указанных позициях. Отсчет начинается с 0 справа налево.

Бинарная дизъюнкция:

0 OR 0 = 0
0 OR 1 = 1
1 OR 0 = 1
1 OR 1 = 1

**Синтаксис**

``` sql
SELECT bitTestAny(number, index1, index2, index3, index4, ...)
```

**Аргументы**

-   `number` – целое число.
-   `index1`, `index2`, `index3`, `index4` – позиции бита.

**Возвращаемое значение**

Возвращает результат логической дизъюнкции.

Тип: `UInt8`.

**Пример**

Например, число 43 в двоичной системе счисления равно: 101011.

Запрос:

``` sql
SELECT bitTestAny(43, 0, 2);
```

Результат:

``` text
┌─bitTestAny(43, 0, 2)─┐
│                    1 │
└──────────────────────┘
```

Другой пример:

Запрос:

``` sql
SELECT bitTestAny(43, 4, 2);
```

Результат:

``` text
┌─bitTestAny(43, 4, 2)─┐
│                    0 │
└──────────────────────┘
```

## bitCount {#bitcount}

Подсчитывает количество равных единице бит в числе.

**Синтаксис**

``` sql
bitCount(x)
```

**Аргументы**

-   `x` — [целое число](../../sql-reference/functions/bit-functions.md) или [число с плавающей запятой](../../sql-reference/functions/bit-functions.md). Функция использует представление числа в памяти, что позволяет поддержать числа с плавающей запятой.

**Возвращаемое значение**

-   Количество равных единице бит во входном числе.

Функция не преобразует входное значение в более крупный тип ([sign extension](https://en.wikipedia.org/wiki/Sign_extension)). Поэтому, например, `bitCount(toUInt8(-1)) = 8`.

Тип: `UInt8`.

**Пример**

Возьмём к примеру число 333. Его бинарное представление — 0000000101001101.

Запрос:

``` sql
SELECT bitCount(333);
```

Результат:

``` text
┌─bitCount(100)─┐
│             5 │
└───────────────┘
```

## bitHammingDistance {#bithammingdistance}

Возвращает [расстояние Хэмминга](https://ru.wikipedia.org/wiki/%D0%A0%D0%B0%D1%81%D1%81%D1%82%D0%BE%D1%8F%D0%BD%D0%B8%D0%B5_%D0%A5%D1%8D%D0%BC%D0%BC%D0%B8%D0%BD%D0%B3%D0%B0) между битовыми представлениями двух целых чисел. Может быть использовано с функциями [SimHash](../../sql-reference/functions/hash-functions.md#ngramsimhash) для проверки двух строк на схожесть. Чем меньше расстояние, тем больше вероятность, что строки совпадают.

**Синтаксис**

``` sql
bitHammingDistance(int1, int2)
```

**Аргументы**

-   `int1` — первое целое число. [Int64](../../sql-reference/data-types/int-uint.md).
-   `int2` — второе целое число. [Int64](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Расстояние Хэмминга.

Тип: [UInt8](../../sql-reference/data-types/int-uint.md).

**Примеры**

Запрос:

``` sql
SELECT bitHammingDistance(111, 121);
```

Результат:

``` text
┌─bitHammingDistance(111, 121)─┐
│                            3 │
└──────────────────────────────┘
```

Используя [SimHash](../../sql-reference/functions/hash-functions.md#ngramsimhash):

``` sql
SELECT bitHammingDistance(ngramSimHash('cat ate rat'), ngramSimHash('rat ate cat'));
```

Результат:

``` text
┌─bitHammingDistance(ngramSimHash('cat ate rat'), ngramSimHash('rat ate cat'))─┐
│                                                                            5 │
└──────────────────────────────────────────────────────────────────────────────┘
```
