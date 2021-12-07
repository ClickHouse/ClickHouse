---
toc_priority: 37
toc_title: "Логические функции"
---

# Логические функции {#logicheskie-funktsii}

Логические функции производят логические операции над любыми числовыми типами, а возвращают число типа [UInt8](../../sql-reference/data-types/int-uint.md), равное 0, 1, а в некоторых случаях `NULL`.

Ноль в качестве аргумента считается `ложью`, а любое ненулевое значение — `истиной`.

## and {#logical-and-function}

Вычисляет результат логической конъюнкции между двумя и более значениями. Соответствует [оператору логического "И"](../../sql-reference/operators/index.md#logical-and-operator).

**Синтаксис**

``` sql
and(val1, val2...)
```

**Аргументы**

-   `val1, val2, ...` — список из как минимум двух значений. [Int](../../sql-reference/data-types/int-uint.md), [UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) или [Nullable](../../sql-reference/data-types/nullable.md). 

**Возвращаемое значение**

-   `0`, если среди аргументов есть хотя бы один нуль.
-   `NULL`, если среди аргументов нет нулей, но есть хотя бы один `NULL`.
-   `1`, в остальных случаях.

Тип: [UInt8](../../sql-reference/data-types/int-uint.md) или [Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md)).

**Пример**

Запрос:

``` sql
SELECT and(0, 1, -2);
```

Результат:

``` text
┌─and(0, 1, -2)─┐
│             0 │
└───────────────┘
```

Со значениями `NULL`:

``` sql
SELECT and(NULL, 1, 10, -2);
```

Результат:

``` text
┌─and(NULL, 1, 10, -2)─┐
│                 ᴺᵁᴸᴸ │
└──────────────────────┘
```

## or {#logical-or-function}

Вычисляет результат логической дизъюнкции между двумя и более значениями. Соответствует [оператору логического "ИЛИ"](../../sql-reference/operators/index.md#logical-or-operator).

**Синтаксис**

``` sql
and(val1, val2...)
```

**Аргументы**

-   `val1, val2, ...` — список из как минимум двух значений. [Int](../../sql-reference/data-types/int-uint.md), [UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) или [Nullable](../../sql-reference/data-types/nullable.md). 

**Returned value**

-   `1`, если среди аргументов есть хотя бы одно ненулевое число.
-   `0`, если среди аргументов только нули.
-   `NULL`, если среди аргументов нет ненулевых значений, и есть `NULL`.

Тип: [UInt8](../../sql-reference/data-types/int-uint.md) или [Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md)).

**Пример**

Запрос:

``` sql
SELECT or(1, 0, 0, 2, NULL);
```

Результат:

``` text
┌─or(1, 0, 0, 2, NULL)─┐
│                    1 │
└──────────────────────┘
```

Со значениями `NULL`:

``` sql
SELECT or(0, NULL);
```

Результат:

``` text
┌─or(0, NULL)─┐
│        ᴺᵁᴸᴸ │
└─────────────┘
```

## not {#logical-not-function}

Вычисляет результат логического отрицания аргумента. Соответствует [оператору логического отрицания](../../sql-reference/operators/index.md#logical-negation-operator).

**Синтаксис**

``` sql
not(val);
```

**Аргументы**

-   `val` — значение. [Int](../../sql-reference/data-types/int-uint.md), [UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) или [Nullable](../../sql-reference/data-types/nullable.md). 

**Возвращаемое значение**

-   `1`, если `val` — это `0`.
-   `0`, если `val` — это ненулевое число.
-   `NULL`, если `val` — это `NULL`.

Тип: [UInt8](../../sql-reference/data-types/int-uint.md) или [Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md)).

**Пример**

Запрос:

``` sql
SELECT NOT(1);
```

Результат:

``` test
┌─not(1)─┐
│      0 │
└────────┘
```

## xor {#logical-xor-function}

Вычисляет результат логической исключающей дизъюнкции между двумя и более значениями. При более чем двух значениях функция работает так: сначала вычисляет `XOR` для первых двух значений, а потом использует полученный результат при вычислении `XOR` со следующим значением и так далее.

**Синтаксис**

``` sql
xor(val1, val2...)
```

**Аргументы**

-   `val1, val2, ...` — список из как минимум двух значений. [Int](../../sql-reference/data-types/int-uint.md), [UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) или [Nullable](../../sql-reference/data-types/nullable.md). 

**Returned value**

-   `1`, для двух значений: если одно из значений является нулем, а второе нет.
-   `0`, для двух значений: если оба значения одновременно нули или ненулевые числа.
-   `NULL`, если среди аргументов хотя бы один `NULL`.

Тип: [UInt8](../../sql-reference/data-types/int-uint.md) or [Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md)).

**Пример**

Запрос:

``` sql
SELECT xor(0, 1, 1);
```

Результат:

``` text
┌─xor(0, 1, 1)─┐
│            0 │
└──────────────┘
```
