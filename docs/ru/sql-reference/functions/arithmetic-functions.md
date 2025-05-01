---
slug: /ru/sql-reference/functions/arithmetic-functions
sidebar_position: 34
sidebar_label: "Арифметические функции"
---

# Арифметические функции {#arifmeticheskie-funktsii}

Для всех арифметических функций, тип результата вычисляется, как минимальный числовой тип, который может вместить результат, если такой тип есть. Минимум берётся одновременно по числу бит, знаковости и «плавучести». Если бит не хватает, то берётся тип максимальной битности.

Пример:

``` sql
SELECT toTypeName(0), toTypeName(0 + 0), toTypeName(0 + 0 + 0), toTypeName(0 + 0 + 0 + 0)
```

``` text
┌─toTypeName(0)─┬─toTypeName(plus(0, 0))─┬─toTypeName(plus(plus(0, 0), 0))─┬─toTypeName(plus(plus(plus(0, 0), 0), 0))─┐
│ UInt8         │ UInt16                 │ UInt32                          │ UInt64                                   │
└───────────────┴────────────────────────┴─────────────────────────────────┴──────────────────────────────────────────┘
```

Арифметические функции работают для любой пары типов из UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.

Переполнение производится также, как в C++.

## plus(a, b), оператор a + b {#plusa-b-operator-a-b}

Вычисляет сумму чисел.
Также можно складывать целые числа с датой и датой-с-временем. В случае даты, прибавление целого числа означает прибавление соответствующего количества дней. В случае даты-с-временем - прибавление соответствующего количества секунд.

## minus(a, b), оператор a - b {#minusa-b-operator-a-b}

Вычисляет разность чисел. Результат всегда имеет знаковый тип.

Также можно вычитать целые числа из даты и даты-с-временем. Смысл аналогичен - смотрите выше для plus.

## multiply(a, b), оператор a \* b {#multiplya-b-operator-a-b}

Вычисляет произведение чисел.

## divide(a, b), оператор a / b {#dividea-b-operator-a-b}

Вычисляет частное чисел. Тип результата всегда является типом с плавающей запятой.
То есть, деление не целочисленное. Для целочисленного деления, используйте функцию intDiv.
При делении на ноль получится inf, -inf или nan.

## intDiv(a, b) {#intdiva-b}

Вычисляет частное чисел. Деление целочисленное, с округлением вниз (по абсолютному значению).
При делении на ноль или при делении минимального отрицательного числа на минус единицу, кидается исключение.

## intDivOrZero(a, b) {#intdivorzeroa-b}

Отличается от intDiv тем, что при делении на ноль или при делении минимального отрицательного числа на минус единицу, возвращается ноль.

## modulo(a, b), оператор a % b {#modulo}

Вычисляет остаток от деления.
Тип результата - целое число, если оба аргумента - целые числа. Если один из аргументов является числом с плавающей точкой, результатом будет число с плавающей точкой.
Берётся остаток в том же смысле, как это делается в C++. По факту, для отрицательных чисел, используется truncated division.
При делении на ноль или при делении минимального отрицательного числа на минус единицу, кидается исключение.

## moduloOrZero(a, b) {#modulo-or-zero}

В отличие от [modulo](#modulo), возвращает ноль при делении на ноль.

## negate(a), оператор -a {#negatea-operator-a}

Вычисляет число, обратное по знаку. Результат всегда имеет знаковый тип.

## abs(a) {#arithm_func-abs}

Вычисляет абсолютное значение для числа a. То есть, если a \< 0, то возвращает -a.
Для беззнаковых типов ничего не делает. Для чисел типа целых со знаком, возвращает число беззнакового типа.

## gcd(a, b) {#gcda-b}

Вычисляет наибольший общий делитель чисел.
При делении на ноль или при делении минимального отрицательного числа на минус единицу, кидается исключение.

## lcm(a, b) {#lcma-b}

Вычисляет наименьшее общее кратное чисел.
При делении на ноль или при делении минимального отрицательного числа на минус единицу, кидается исключение.


## max2 {#max2}

Сравнивает два числа и возвращает максимум. Возвращаемое значение приводится к типу [Float64](../../sql-reference/data-types/float.md).

**Синтаксис**

```sql
max2(value1, value2)
```

**Аргументы**

-   `value1` — первое число. [Int/UInt](../../sql-reference/data-types/int-uint.md) или [Float](../../sql-reference/data-types/float.md).
-   `value2` — второе число. [Int/UInt](../../sql-reference/data-types/int-uint.md) или [Float](../../sql-reference/data-types/float.md).

**Возвращаемое значение**

-   Максимальное значение среди двух чисел.

Тип: [Float](../../sql-reference/data-types/float.md).

**Пример**

Запрос:

```sql
SELECT max2(-1, 2);
```

Результат:

```text
┌─max2(-1, 2)─┐
│           2 │
└─────────────┘
```

## min2 {#min2}

Сравнивает два числа и возвращает минимум. Возвращаемое значение приводится к типу [Float64](../../sql-reference/data-types/float.md).

**Синтаксис**

```sql
min2(value1, value2)
```

**Аргументы**

-   `value1` — первое число. [Int/UInt](../../sql-reference/data-types/int-uint.md) или [Float](../../sql-reference/data-types/float.md).
-   `value2` — второе число. [Int/UInt](../../sql-reference/data-types/int-uint.md) или [Float](../../sql-reference/data-types/float.md).

**Возвращаемое значение**

-   Минимальное значение среди двух чисел.

Тип: [Float](../../sql-reference/data-types/float.md).

**Пример**

Запрос:

```sql
SELECT min2(-1, 2);
```

Результат:

```text
┌─min2(-1, 2)─┐
│          -1 │
└─────────────┘
```

## multiplyDecimal(a, b[, result_scale])

Совершает умножение двух Decimal. Результат будет иметь тип [Decimal256](../../sql-reference/data-types/decimal.md).
Scale (размер дробной части) результат можно явно задать аргументом `result_scale`  (целочисленная константа из интервала `[0, 76]`).
Если этот аргумент не задан, то scale результата будет равен наибольшему из scale обоих аргументов.

**Синтаксис**

```sql
multiplyDecimal(a, b[, result_scale])
```

:::note    
Эта функция работает гораздо медленнее обычной `multiply`.
В случае, если нет необходимости иметь фиксированную точность и/или нужны быстрые вычисления, следует использовать [multiply](#multiply).
:::

**Аргументы**

-   `a` — Первый сомножитель/делимое: [Decimal](../../sql-reference/data-types/decimal.md).
-   `b` — Второй сомножитель/делитель: [Decimal](../../sql-reference/data-types/decimal.md).
-   `result_scale` — Scale результата: [Int/UInt](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Результат умножения с заданным scale.

Тип: [Decimal256](../../sql-reference/data-types/decimal.md).

**Примеры**

```sql
SELECT multiplyDecimal(toDecimal256(-12, 0), toDecimal32(-2.1, 1), 1);
```

```text
┌─multiplyDecimal(toDecimal256(-12, 0), toDecimal32(-2.1, 1), 1)─┐
│                                                           25.2 │
└────────────────────────────────────────────────────────────────┘
```

**Отличие от стандартных функций**
```sql
SELECT toDecimal64(-12.647, 3) * toDecimal32(2.1239, 4);
SELECT toDecimal64(-12.647, 3) as a, toDecimal32(2.1239, 4) as b, multiplyDecimal(a, b);
```

```text
┌─multiply(toDecimal64(-12.647, 3), toDecimal32(2.1239, 4))─┐
│                                               -26.8609633 │
└───────────────────────────────────────────────────────────┘
┌─multiplyDecimal(toDecimal64(-12.647, 3), toDecimal32(2.1239, 4))─┐
│                                                         -26.8609 │
└──────────────────────────────────────────────────────────────────┘
```

```sql
SELECT
    toDecimal64(-12.647987876, 9) AS a,
    toDecimal64(123.967645643, 9) AS b,
    multiplyDecimal(a, b);

SELECT
    toDecimal64(-12.647987876, 9) AS a,
    toDecimal64(123.967645643, 9) AS b,
    a * b;
```

```text
┌─────────────a─┬─────────────b─┬─multiplyDecimal(toDecimal64(-12.647987876, 9), toDecimal64(123.967645643, 9))─┐
│ -12.647987876 │ 123.967645643 │                                                               -1567.941279108 │
└───────────────┴───────────────┴───────────────────────────────────────────────────────────────────────────────┘

Received exception from server (version 22.11.1):
Code: 407. DB::Exception: Received from localhost:9000. DB::Exception: Decimal math overflow: While processing toDecimal64(-12.647987876, 9) AS a, toDecimal64(123.967645643, 9) AS b, a * b. (DECIMAL_OVERFLOW)
```

## divideDecimal(a, b[, result_scale])

Совершает деление двух Decimal. Результат будет иметь тип [Decimal256](../../sql-reference/data-types/decimal.md).
Scale (размер дробной части) результат можно явно задать аргументом `result_scale`  (целочисленная константа из интервала `[0, 76]`).
Если этот аргумент не задан, то scale результата будет равен наибольшему из scale обоих аргументов.

**Синтаксис**

```sql
divideDecimal(a, b[, result_scale])
```

:::note    
Эта функция работает гораздо медленнее обычной `divide`.
В случае, если нет необходимости иметь фиксированную точность и/или нужны быстрые вычисления, следует использовать [divide](#divide).
:::

**Аргументы**

-   `a` — Первый сомножитель/делимое: [Decimal](../../sql-reference/data-types/decimal.md).
-   `b` — Второй сомножитель/делитель: [Decimal](../../sql-reference/data-types/decimal.md).
-   `result_scale` — Scale результата: [Int/UInt](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Результат деления с заданным scale.

Тип: [Decimal256](../../sql-reference/data-types/decimal.md).

**Примеры**

```sql
SELECT divideDecimal(toDecimal256(-12, 0), toDecimal32(2.1, 1), 10);
```

```text
┌─divideDecimal(toDecimal256(-12, 0), toDecimal32(2.1, 1), 10)─┐
│                                                -5.7142857142 │
└──────────────────────────────────────────────────────────────┘
```

**Отличие от стандартных функций**
```sql
SELECT toDecimal64(-12, 1) / toDecimal32(2.1, 1);
SELECT toDecimal64(-12, 1) as a, toDecimal32(2.1, 1) as b, divideDecimal(a, b, 1), divideDecimal(a, b, 5);
```

```text
┌─divide(toDecimal64(-12, 1), toDecimal32(2.1, 1))─┐
│                                             -5.7 │
└──────────────────────────────────────────────────┘

┌───a─┬───b─┬─divideDecimal(toDecimal64(-12, 1), toDecimal32(2.1, 1), 1)─┬─divideDecimal(toDecimal64(-12, 1), toDecimal32(2.1, 1), 5)─┐
│ -12 │ 2.1 │                                                       -5.7 │                                                   -5.71428 │
└─────┴─────┴────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────┘
```

```sql
SELECT toDecimal64(-12, 0) / toDecimal32(2.1, 1);
SELECT toDecimal64(-12, 0) as a, toDecimal32(2.1, 1) as b, divideDecimal(a, b, 1), divideDecimal(a, b, 5);
```

```text
DB::Exception: Decimal result's scale is less than argument's one: While processing toDecimal64(-12, 0) / toDecimal32(2.1, 1). (ARGUMENT_OUT_OF_BOUND)

┌───a─┬───b─┬─divideDecimal(toDecimal64(-12, 0), toDecimal32(2.1, 1), 1)─┬─divideDecimal(toDecimal64(-12, 0), toDecimal32(2.1, 1), 5)─┐
│ -12 │ 2.1 │                                                       -5.7 │                                                   -5.71428 │
└─────┴─────┴────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────┘
```
