---
toc_priority: 51
toc_title: "\u0424\u0443\u043d\u043a\u0446\u0438\u0438\u0020\u0433\u0435\u043d\u0435\u0440\u0430\u0446\u0438\u0438\u0020\u043f\u0441\u0435\u0432\u0434\u043e\u0441\u043b\u0443\u0447\u0430\u0439\u043d\u044b\u0445\u0020\u0447\u0438\u0441\u0435\u043b"
---

# Функции генерации псевдослучайных чисел {#functions-for-generating-pseudo-random-numbers}

Используются не криптографические генераторы псевдослучайных чисел.

Все функции принимают ноль аргументов или один аргумент.
В случае, если передан аргумент - он может быть любого типа, и его значение никак не используется.
Этот аргумент нужен только для того, чтобы предотвратить склейку одинаковых выражений - чтобы две разные записи одной функции возвращали разные столбцы, с разными случайными числами.

## rand {#rand}

Возвращает псевдослучайное число типа UInt32, равномерно распределённое среди всех чисел типа UInt32.
Используется linear congruential generator.

## rand64 {#rand64}

Возвращает псевдослучайное число типа UInt64, равномерно распределённое среди всех чисел типа UInt64.
Используется linear congruential generator.

## randConstant {#randconstant}

Создает константный столбец с псевдослучайным значением.

**Синтаксис**

``` sql
randConstant([x])
```

**Параметры**

-   `x` — [Выражение](../syntax.md#syntax-expressions), возвращающее значение одного из [поддерживаемых типов данных](../data-types/index.md#data_types). Значение используется, чтобы избежать [склейки одинаковых выражений](index.md#common-subexpression-elimination), если функция вызывается несколько раз в одном запросе. Необязательный параметр.

**Возвращаемое значение**

-   Псевдослучайное число.

Тип: [UInt32](../data-types/int-uint.md).

**Пример**

Запрос:

``` sql
SELECT rand(), rand(1), rand(number), randConstant(), randConstant(1), randConstant(number)
FROM numbers(3)
```

Результат:

``` text
┌─────rand()─┬────rand(1)─┬─rand(number)─┬─randConstant()─┬─randConstant(1)─┬─randConstant(number)─┐
│ 3047369878 │ 4132449925 │   4044508545 │     2740811946 │      4229401477 │           1924032898 │
│ 2938880146 │ 1267722397 │   4154983056 │     2740811946 │      4229401477 │           1924032898 │
│  956619638 │ 4238287282 │   1104342490 │     2740811946 │      4229401477 │           1924032898 │
└────────────┴────────────┴──────────────┴────────────────┴─────────────────┴──────────────────────┘
```

# Случайные функции для работы со строками {#random-functions-for-working-with-strings}

## randomString {#random-string}

## randomFixedString {#random-fixed-string}

## randomPrintableASCII {#random-printable-ascii}

## randomStringUTF8 {#random-string-utf8}

## fuzzBits {#fuzzbits}

**Синтаксис**

``` sql
fuzzBits([s], [prob])
```
Инвертирует каждый бит `s` с вероятностью `prob`.

**Параметры**

- `s` — `String` or `FixedString`
- `prob` — constant `Float32/64`

**Возвращаемое значение**

Измененная случайным образом строка с тем же типом, что и `s`.

**Пример**

Запрос:

``` sql
SELECT fuzzBits(materialize('abacaba'), 0.1)
FROM numbers(3)
```

Результат:

``` text
┌─fuzzBits(materialize('abacaba'), 0.1)─┐
│ abaaaja                               │
│ a*cjab+                               │
│ aeca2A                                │
└───────────────────────────────────────┘
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/functions/random_functions/) <!--hide-->
