---
sidebar_position: 68
sidebar_label: Функции для работы с кортежами
---

# Функции для работы с кортежами {#tuple-functions}

## tuple {#tuple}

Функция, позволяющая сгруппировать несколько столбцов.
Для столбцов, имеющих типы T1, T2, … возвращает кортеж типа Tuple(T1, T2, …), содержащий эти столбцы. Выполнение функции ничего не стоит.
Кортежи обычно используются как промежуточное значение в качестве аргумента операторов IN, или для создания списка формальных параметров лямбда-функций. Кортежи не могут быть записаны в таблицу.

С помощью функции реализуется оператор `(x, y, …)`.

**Синтаксис**

``` sql
tuple(x, y, …)
```

## tupleElement {#tupleelement}

Функция, позволяющая достать столбец из кортежа.
N - индекс столбца начиная с 1. N должно быть константой. N должно быть целым строго положительным числом не большим размера кортежа.
Выполнение функции ничего не стоит.

С помощью функции реализуется оператор `x.N`.

**Синтаксис**

``` sql
tupleElement(tuple, n)
```

## untuple {#untuple}

Выполняет синтаксическую подстановку элементов [кортежа](../../sql-reference/data-types/tuple.md#tuplet1-t2) в место вызова.

**Синтаксис**

``` sql
untuple(x)
```

Чтобы пропустить некоторые столбцы в результате запроса, вы можете использовать выражение `EXCEPT`.

**Аргументы**

-   `x` — функция `tuple`, столбец или кортеж элементов. [Tuple](../../sql-reference/data-types/tuple.md).

**Возвращаемое значение**

-   Нет.

**Примеры**

Входная таблица:

``` text
┌─key─┬─v1─┬─v2─┬─v3─┬─v4─┬─v5─┬─v6────────┐
│   1 │ 10 │ 20 │ 40 │ 30 │ 15 │ (33,'ab') │
│   2 │ 25 │ 65 │ 70 │ 40 │  6 │ (44,'cd') │
│   3 │ 57 │ 30 │ 20 │ 10 │  5 │ (55,'ef') │
│   4 │ 55 │ 12 │  7 │ 80 │ 90 │ (66,'gh') │
│   5 │ 30 │ 50 │ 70 │ 25 │ 55 │ (77,'kl') │
└─────┴────┴────┴────┴────┴────┴───────────┘
```

Пример использования столбца типа `Tuple` в качестве параметра функции `untuple`:

Запрос:

``` sql
SELECT untuple(v6) FROM kv;
```

Результат:

``` text
┌─_ut_1─┬─_ut_2─┐
│    33 │ ab    │
│    44 │ cd    │
│    55 │ ef    │
│    66 │ gh    │
│    77 │ kl    │
└───────┴───────┘
```

Пример использования выражения `EXCEPT`:

Запрос:

``` sql
SELECT untuple((* EXCEPT (v2, v3),)) FROM kv;
```

Результат:

``` text
┌─key─┬─v1─┬─v4─┬─v5─┬─v6────────┐
│   1 │ 10 │ 30 │ 15 │ (33,'ab') │
│   2 │ 25 │ 40 │  6 │ (44,'cd') │
│   3 │ 57 │ 10 │  5 │ (55,'ef') │
│   4 │ 55 │ 80 │ 90 │ (66,'gh') │
│   5 │ 30 │ 25 │ 55 │ (77,'kl') │
└─────┴────┴────┴────┴───────────┘
```

**Смотрите также**

-   [Tuple](../../sql-reference/data-types/tuple.md)

## tupleHammingDistance {#tuplehammingdistance}

Возвращает [расстояние Хэмминга](https://ru.wikipedia.org/wiki/%D0%A0%D0%B0%D1%81%D1%81%D1%82%D0%BE%D1%8F%D0%BD%D0%B8%D0%B5_%D0%A5%D1%8D%D0%BC%D0%BC%D0%B8%D0%BD%D0%B3%D0%B0) между двумя кортежами одинакового размера.

**Синтаксис**

``` sql
tupleHammingDistance(tuple1, tuple2)
```

**Аргументы**

-   `tuple1` — первый кортеж. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — второй кортеж. [Tuple](../../sql-reference/data-types/tuple.md).

Кортежи должны иметь одинаковый размер и тип элементов.

**Возвращаемое значение**

-   Расстояние Хэмминга.

Тип: [UInt8](../../sql-reference/data-types/int-uint.md).

**Примеры**

Запрос:

``` sql
SELECT tupleHammingDistance((1, 2, 3), (3, 2, 1)) AS HammingDistance;
```

Результат:

``` text
┌─HammingDistance─┐
│               2 │
└─────────────────┘
```

Может быть использовано с функциями [MinHash](../../sql-reference/functions/hash-functions.md#ngramminhash) для проверки строк на совпадение:

``` sql
SELECT tupleHammingDistance(wordShingleMinHash(string), wordShingleMinHashCaseInsensitive(string)) as HammingDistance FROM (SELECT 'Clickhouse is a column-oriented database management system for online analytical processing of queries.' AS string);
```

Результат:

``` text
┌─HammingDistance─┐
│               2 │
└─────────────────┘
```

## tupleToNameValuePairs {#tupletonamevaluepairs}

Приводит именованный кортеж к списку пар (имя, значение). Для `Tuple(a T, b T, ..., c T)` возвращает `Array(Tuple(String, T), ...)`, где `Strings` — это названия именованных полей, а `T` — это соответствующие значения. Все значения в кортеже должны быть одинакового типа.

**Синтаксис**

``` sql
tupleToNameValuePairs(tuple)
```

**Аргументы**

-   `tuple` — именованный кортеж. [Tuple](../../sql-reference/data-types/tuple.md) с любым типом значений.

**Возвращаемое значение**

-   Список пар (имя, значение).

Тип: [Array](../../sql-reference/data-types/array.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md), ...)).

**Пример**

Запрос:

``` sql
CREATE TABLE tupletest (`col` Tuple(user_ID UInt64, session_ID UInt64) ENGINE = Memory;

INSERT INTO tupletest VALUES (tuple( 100, 2502)), (tuple(1,100));

SELECT tupleToNameValuePairs(col) FROM tupletest;
``` 

Результат:

``` text
┌─tupleToNameValuePairs(col)────────────┐
│ [('user_ID',100),('session_ID',2502)] │
│ [('user_ID',1),('session_ID',100)]    │
└───────────────────────────────────────┘
```

С помощью этой функции можно выводить столбцы в виде строк:

``` sql
CREATE TABLE tupletest (`col` Tuple(CPU Float64, Memory Float64, Disk Float64)) ENGINE = Memory;

INSERT INTO tupletest VALUES(tuple(3.3, 5.5, 6.6));

SELECT arrayJoin(tupleToNameValuePairs(col))FROM tupletest;
```

Результат:

``` text
┌─arrayJoin(tupleToNameValuePairs(col))─┐
│ ('CPU',3.3)                           │
│ ('Memory',5.5)                        │
│ ('Disk',6.6)                          │
└───────────────────────────────────────┘
```

Если в функцию передается обычный кортеж, ClickHouse использует индексы значений в качестве имен:

``` sql
SELECT tupleToNameValuePairs(tuple(3, 2, 1));
```

Результат:

``` text
┌─tupleToNameValuePairs(tuple(3, 2, 1))─┐
│ [('1',3),('2',2),('3',1)]             │
└───────────────────────────────────────┘
```

## tuplePlus {#tupleplus}

Вычисляет сумму соответствующих значений двух кортежей одинакового размера.

**Синтаксис**

```sql
tuplePlus(tuple1, tuple2)
```

Синоним: `vectorSum`.

**Аргументы**

-   `tuple1` — первый кортеж. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — второй кортеж. [Tuple](../../sql-reference/data-types/tuple.md).

**Возвращаемое значение**

-   Кортеж с суммами.

Тип: [Tuple](../../sql-reference/data-types/tuple.md).

**Пример**

Запрос:

```sql
SELECT tuplePlus((1, 2), (2, 3));
```

Результат:

```text
┌─tuplePlus((1, 2), (2, 3))─┐
│ (3,5)                     │
└───────────────────────────┘
```

## tupleMinus {#tupleminus}

Вычисляет разность соответствующих значений двух кортежей одинакового размера.

**Синтаксис**

```sql
tupleMinus(tuple1, tuple2)
```

Синоним: `vectorDifference`.

**Аргументы**

-   `tuple1` — первый кортеж. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — второй кортеж. [Tuple](../../sql-reference/data-types/tuple.md).

**Возвращаемое значение**

-   Кортеж с разностями.

Тип: [Tuple](../../sql-reference/data-types/tuple.md).

**Пример**

Запрос:

```sql
SELECT tupleMinus((1, 2), (2, 3));
```

Результат:

```text
┌─tupleMinus((1, 2), (2, 3))─┐
│ (-1,-1)                    │
└────────────────────────────┘
```

## tupleMultiply {#tuplemultiply}

Вычисляет произведение соответствующих значений двух кортежей одинакового размера.

**Синтаксис**

```sql
tupleMultiply(tuple1, tuple2)
```

**Аргументы**

-   `tuple1` — первый кортеж. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — второй кортеж. [Tuple](../../sql-reference/data-types/tuple.md).

**Возвращаемое значение**

-   Кортеж с произведениями.

Тип: [Tuple](../../sql-reference/data-types/tuple.md).

**Пример**

Запрос:

```sql
SELECT tupleMultiply((1, 2), (2, 3));
```

Результат:

```text
┌─tupleMultiply((1, 2), (2, 3))─┐
│ (2,6)                         │
└───────────────────────────────┘
```

## tupleDivide {#tupledivide}

Вычисляет частное соответствующих значений двух кортежей одинакового размера. Обратите внимание, что при делении на ноль возвращается значение `inf`.

**Синтаксис**

```sql
tupleDivide(tuple1, tuple2)
```

**Аргументы**

-   `tuple1` — первый кортеж. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — второй кортеж. [Tuple](../../sql-reference/data-types/tuple.md).

**Возвращаемое значение**

-   Кортеж с частными.

Тип: [Tuple](../../sql-reference/data-types/tuple.md).

**Пример**

Запрос:

```sql
SELECT tupleDivide((1, 2), (2, 3));
```

Результат:

```text
┌─tupleDivide((1, 2), (2, 3))─┐
│ (0.5,0.6666666666666666)    │
└─────────────────────────────┘
```

## tupleNegate {#tuplenegate}

Применяет отрицание ко всем значениям кортежа.

**Синтаксис**

```sql
tupleNegate(tuple)
```

**Аргументы**

-   `tuple` — кортеж. [Tuple](../../sql-reference/data-types/tuple.md).

**Возвращаемое значение**

-   Кортеж с результатом отрицания.

Тип: [Tuple](../../sql-reference/data-types/tuple.md).

**Пример**

Запрос:

```sql
SELECT tupleNegate((1, 2));
```

Результат:

```text
┌─tupleNegate((1, 2))─┐
│ (-1,-2)             │
└─────────────────────┘
```

## tupleMultiplyByNumber {#tuplemultiplybynumber}

Возвращает кортеж, в котором значения всех элементов умножены на заданное число.

**Синтаксис**

```sql
tupleMultiplyByNumber(tuple, number)
```

**Аргументы**

-   `tuple` — кортеж. [Tuple](../../sql-reference/data-types/tuple.md).
-   `number` — множитель. [Int/UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) или [Decimal](../../sql-reference/data-types/decimal.md).

**Возвращаемое значение**

-   Кортеж с результатами умножения на число.

Тип: [Tuple](../../sql-reference/data-types/tuple.md).

**Пример**

Запрос:

```sql
SELECT tupleMultiplyByNumber((1, 2), -2.1);
```

Результат:

```text
┌─tupleMultiplyByNumber((1, 2), -2.1)─┐
│ (-2.1,-4.2)                         │
└─────────────────────────────────────┘
```

## tupleDivideByNumber {#tupledividebynumber}

Возвращает кортеж, в котором значения всех элементов поделены на заданное число. Обратите внимание, что при делении на ноль возвращается значение `inf`.

**Синтаксис**

```sql
tupleDivideByNumber(tuple, number)
```

**Аргументы**

-   `tuple` — кортеж. [Tuple](../../sql-reference/data-types/tuple.md).
-   `number` — делитель. [Int/UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).

**Возвращаемое значение**

-   Кортеж с результатами деления на число.

Тип: [Tuple](../../sql-reference/data-types/tuple.md).

**Пример**

Запрос:

```sql
SELECT tupleDivideByNumber((1, 2), 0.5);
```

Результат:

```text
┌─tupleDivideByNumber((1, 2), 0.5)─┐
│ (2,4)                            │
└──────────────────────────────────┘
```

## dotProduct {#dotproduct}

Вычисляет скалярное произведение двух кортежей одинакового размера.

**Синтаксис**

```sql
dotProduct(tuple1, tuple2)
```

Синоним: `scalarProduct`.

**Аргументы**

-   `tuple1` — первый кортеж. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — второй кортеж. [Tuple](../../sql-reference/data-types/tuple.md).

**Возвращаемое значение**

-   Скалярное произведение.

Тип: [Int/UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) или [Decimal](../../sql-reference/data-types/decimal.md).

**Пример**

Запрос:

```sql
SELECT dotProduct((1, 2), (2, 3));
```

Результат:

```text
┌─dotProduct((1, 2), (2, 3))─┐
│                          8 │
└────────────────────────────┘
```

## L1Norm {#l1norm}

Вычисляет сумму абсолютных значений кортежа.

**Синтаксис**

```sql
L1Norm(tuple)
```

Синоним: `normL1`.

**Аргументы**

-   `tuple` — кортеж. [Tuple](../../sql-reference/data-types/tuple.md).

**Возвращаемое значение**

-   L1-норма или [расстояние городских кварталов](https://ru.wikipedia.org/wiki/%D0%A0%D0%B0%D1%81%D1%81%D1%82%D0%BE%D1%8F%D0%BD%D0%B8%D0%B5_%D0%B3%D0%BE%D1%80%D0%BE%D0%B4%D1%81%D0%BA%D0%B8%D1%85_%D0%BA%D0%B2%D0%B0%D1%80%D1%82%D0%B0%D0%BB%D0%BE%D0%B2).

Тип: [UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) или [Decimal](../../sql-reference/data-types/decimal.md).

**Пример**

Запрос:

```sql
SELECT L1Norm((1, 2));
```

Результат:

```text
┌─L1Norm((1, 2))─┐
│              3 │
└────────────────┘
```

## L2Norm {#l2norm}

Вычисляет квадратный корень из суммы квадратов значений кортежа.

**Синтаксис**

```sql
L2Norm(tuple)
```

Синоним: `normL2`.

**Аргументы**

-   `tuple` — кортеж. [Tuple](../../sql-reference/data-types/tuple.md).

**Возвращаемое значение**

-   L2-норма или [Евклидово расстояние](https://ru.wikipedia.org/wiki/%D0%95%D0%B2%D0%BA%D0%BB%D0%B8%D0%B4%D0%BE%D0%B2%D0%B0_%D0%BC%D0%B5%D1%82%D1%80%D0%B8%D0%BA%D0%B0).

Тип: [Float](../../sql-reference/data-types/float.md).

**Пример**

Запрос:

```sql
SELECT L2Norm((1, 2));
```

Результат:

```text
┌───L2Norm((1, 2))─┐
│ 2.23606797749979 │
└──────────────────┘
```

## LinfNorm {#linfnorm}

Вычисляет максимум из абсолютных значений кортежа.

**Синтаксис**

```sql
LinfNorm(tuple)
```

Синоним: `normLinf`.

**Аргументы**

-   `tuple` — кортеж. [Tuple](../../sql-reference/data-types/tuple.md).

**Возвращаемое значение**

-   Linf-норма или максимальное абсолютное значение.

Тип: [Float](../../sql-reference/data-types/float.md).

**Пример**

Запрос:

```sql
SELECT LinfNorm((1, -2));
```

Результат:

```text
┌─LinfNorm((1, -2))─┐
│                 2 │
└───────────────────┘
```

## LpNorm {#lpnorm}

Возвращает корень степени `p` из суммы абсолютных значений кортежа, возведенных в степень `p`.

**Синтаксис**

```sql
LpNorm(tuple, p)
```

Синоним: `normLp`.

**Аргументы**

-   `tuple` — кортеж. [Tuple](../../sql-reference/data-types/tuple.md).
-   `p` — степень. Возможные значение: любое число из промежутка [1;inf). [UInt](../../sql-reference/data-types/int-uint.md) или [Float](../../sql-reference/data-types/float.md).

**Возвращаемое значение**

-   [Lp-норма](https://ru.wikipedia.org/wiki/%D0%9D%D0%BE%D1%80%D0%BC%D0%B0_(%D0%BC%D0%B0%D1%82%D0%B5%D0%BC%D0%B0%D1%82%D0%B8%D0%BA%D0%B0)#%D0%9D%D0%B5%D0%BA%D0%BE%D1%82%D0%BE%D1%80%D1%8B%D0%B5_%D0%B2%D0%B8%D0%B4%D1%8B_%D0%BC%D0%B0%D1%82%D1%80%D0%B8%D1%87%D0%BD%D1%8B%D1%85_%D0%BD%D0%BE%D1%80%D0%BC)

Тип: [Float](../../sql-reference/data-types/float.md).

**Пример**

Запрос:

```sql
SELECT LpNorm((1, -2),2);
```

Результат:

```text
┌─LpNorm((1, -2), 2)─┐
│   2.23606797749979 │
└────────────────────┘
```

## L1Distance {#l1distance}

Вычисляет расстояние между двумя точками (значения кортежей — координаты точек) в пространстве `L1` ([расстояние городских кварталов](https://ru.wikipedia.org/wiki/%D0%A0%D0%B0%D1%81%D1%81%D1%82%D0%BE%D1%8F%D0%BD%D0%B8%D0%B5_%D0%B3%D0%BE%D1%80%D0%BE%D0%B4%D1%81%D0%BA%D0%B8%D1%85_%D0%BA%D0%B2%D0%B0%D1%80%D1%82%D0%B0%D0%BB%D0%BE%D0%B2)).

**Синтаксис**

```sql
L1Distance(tuple1, tuple2)
```

Синоним: `distanceL1`.

**Аргументы**

-   `tuple1` — первый кортеж. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — второй кортеж. [Tuple](../../sql-reference/data-types/tuple.md).

**Возвращаемое значение**

-   Расстояние в норме L1.

Тип: [Float](../../sql-reference/data-types/float.md).

**Пример**

Запрос:

```sql
SELECT L1Distance((1, 2), (2, 3));
```

Результат:

```text
┌─L1Distance((1, 2), (2, 3))─┐
│                          2 │
└────────────────────────────┘
```

## L2Distance {#l2distance}

Вычисляет расстояние между двумя точками (значения кортежей — координаты точек) в пространстве `L2` ([Евклидово расстояние](https://ru.wikipedia.org/wiki/%D0%95%D0%B2%D0%BA%D0%BB%D0%B8%D0%B4%D0%BE%D0%B2%D0%B0_%D0%BC%D0%B5%D1%82%D1%80%D0%B8%D0%BA%D0%B0)).

**Синтаксис**

```sql
L2Distance(tuple1, tuple2)
```

Синоним: `distanceL2`.

**Аргументы**

-   `tuple1` — первый кортеж. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — второй кортеж. [Tuple](../../sql-reference/data-types/tuple.md).

**Возвращаемое значение**

-   Расстояние в норме L2.

Тип: [Float](../../sql-reference/data-types/float.md).

**Пример**

Запрос:

```sql
SELECT L2Distance((1, 2), (2, 3));
```

Результат:

```text
┌─L2Distance((1, 2), (2, 3))─┐
│         1.4142135623730951 │
└────────────────────────────┘
```

## LinfDistance {#linfdistance}

Вычисляет расстояние между двумя точками (значения кортежей — координаты точек) в пространстве [`L_{inf}`](https://ru.wikipedia.org/wiki/%D0%9D%D0%BE%D1%80%D0%BC%D0%B0_(%D0%BC%D0%B0%D1%82%D0%B5%D0%BC%D0%B0%D1%82%D0%B8%D0%BA%D0%B0)#%D0%9D%D0%B5%D0%BA%D0%BE%D1%82%D0%BE%D1%80%D1%8B%D0%B5_%D0%B2%D0%B8%D0%B4%D1%8B_%D0%BC%D0%B0%D1%82%D1%80%D0%B8%D1%87%D0%BD%D1%8B%D1%85_%D0%BD%D0%BE%D1%80%D0%BC).

**Синтаксис**

```sql
LinfDistance(tuple1, tuple2)
```

Синоним: `distanceLinf`.

**Аргументы**

-   `tuple1` — первый кортеж. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — второй кортеж. [Tuple](../../sql-reference/data-types/tuple.md).

**Возвращаемые значения**

-   Расстояние в норме Linf.

Тип: [Float](../../sql-reference/data-types/float.md).

**Пример**

Запрос:

```sql
SELECT LinfDistance((1, 2), (2, 3));
```

Результат:

```text
┌─LinfDistance((1, 2), (2, 3))─┐
│                            1 │
└──────────────────────────────┘
```

## LpDistance {#lpdistance}

Вычисляет расстояние между двумя точками (значения кортежей — координаты точек) в пространстве [`Lp`](https://ru.wikipedia.org/wiki/%D0%9D%D0%BE%D1%80%D0%BC%D0%B0_(%D0%BC%D0%B0%D1%82%D0%B5%D0%BC%D0%B0%D1%82%D0%B8%D0%BA%D0%B0)#%D0%9D%D0%B5%D0%BA%D0%BE%D1%82%D0%BE%D1%80%D1%8B%D0%B5_%D0%B2%D0%B8%D0%B4%D1%8B_%D0%BC%D0%B0%D1%82%D1%80%D0%B8%D1%87%D0%BD%D1%8B%D1%85_%D0%BD%D0%BE%D1%80%D0%BC).

**Синтаксис**

```sql
LpDistance(tuple1, tuple2, p)
```

Синоним: `distanceLp`.

**Аргументы**

-   `tuple1` — первый кортеж. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — второй кортеж. [Tuple](../../sql-reference/data-types/tuple.md).
-   `p` — степень. Возможные значение: любое число из промежутка [1;inf). [UInt](../../sql-reference/data-types/int-uint.md) или [Float](../../sql-reference/data-types/float.md).

**Возвращаемое значение**

-   Расстояние в норме Lp.

Тип: [Float](../../sql-reference/data-types/float.md).

**Пример**

Запрос:

```sql
SELECT LpDistance((1, 2), (2, 3), 3);
```

Результат:

```text
┌─LpDistance((1, 2), (2, 3), 3)─┐
│            1.2599210498948732 │
└───────────────────────────────┘
```

## L1Normalize {#l1normalize}

Вычисляет единичный вектор для исходного вектора (значения кортежа — координаты вектора) в пространстве `L1` ([расстояние городских кварталов](https://ru.wikipedia.org/wiki/%D0%A0%D0%B0%D1%81%D1%81%D1%82%D0%BE%D1%8F%D0%BD%D0%B8%D0%B5_%D0%B3%D0%BE%D1%80%D0%BE%D0%B4%D1%81%D0%BA%D0%B8%D1%85_%D0%BA%D0%B2%D0%B0%D1%80%D1%82%D0%B0%D0%BB%D0%BE%D0%B2)).

**Синтаксис**

```sql
L1Normalize(tuple)
```

Синоним: `normalizeL1`.

**Аргументы**

-   `tuple` — [Tuple](../../sql-reference/data-types/tuple.md).

**Возвращаемое значение**

-   Единичный вектор.

Тип: кортеж [Tuple](../../sql-reference/data-types/tuple.md) значений [Float](../../sql-reference/data-types/float.md).

**Пример**

Запрос:

```sql
SELECT L1Normalize((1, 2));
```

Результат:

```text
┌─L1Normalize((1, 2))─────────────────────┐
│ (0.3333333333333333,0.6666666666666666) │
└─────────────────────────────────────────┘
```

## L2Normalize {#l2normalize}

Вычисляет единичный вектор для исходного вектора (значения кортежа — координаты вектора) в пространстве `L2` ([Евклидово пространство](https://ru.wikipedia.org/wiki/%D0%95%D0%B2%D0%BA%D0%BB%D0%B8%D0%B4%D0%BE%D0%B2%D0%BE_%D0%BF%D1%80%D0%BE%D1%81%D1%82%D1%80%D0%B0%D0%BD%D1%81%D1%82%D0%B2%D0%BE).

**Синтаксис**

```sql
L2Normalize(tuple)
```

Синоним: `normalizeL1`.

**Аргументы**

-   `tuple` — кортеж. [Tuple](../../sql-reference/data-types/tuple.md).

**Возвращаемое значение**

-   Единичный вектор.

Тип: кортеж [Tuple](../../sql-reference/data-types/tuple.md) значений [Float](../../sql-reference/data-types/float.md).

**Пример**

Запрос:

```sql
SELECT L2Normalize((3, 4));
```

Результат:

```text
┌─L2Normalize((3, 4))─┐
│ (0.6,0.8)           │
└─────────────────────┘
```

## LinfNormalize {#linfnormalize}

Вычисляет единичный вектор для исходного вектора (значения кортежа — координаты вектора) в пространстве [`L_{inf}`](https://ru.wikipedia.org/wiki/%D0%9D%D0%BE%D1%80%D0%BC%D0%B0_(%D0%BC%D0%B0%D1%82%D0%B5%D0%BC%D0%B0%D1%82%D0%B8%D0%BA%D0%B0)#%D0%9D%D0%B5%D0%BA%D0%BE%D1%82%D0%BE%D1%80%D1%8B%D0%B5_%D0%B2%D0%B8%D0%B4%D1%8B_%D0%BC%D0%B0%D1%82%D1%80%D0%B8%D1%87%D0%BD%D1%8B%D1%85_%D0%BD%D0%BE%D1%80%D0%BC).

**Синтаксис**

```sql
LinfNormalize(tuple)
```

Синоним: `normalizeLinf `.

**Аргументы**

-   `tuple` — кортеж. [Tuple](../../sql-reference/data-types/tuple.md).

**Возвращаемое значение**

-   Единичный вектор.

Тип: кортеж [Tuple](../../sql-reference/data-types/tuple.md) значений [Float](../../sql-reference/data-types/float.md).

**Пример**

Запрос:

```sql
SELECT LinfNormalize((3, 4));
```

Результат:

```text
┌─LinfNormalize((3, 4))─┐
│ (0.75,1)              │
└───────────────────────┘
```

## LpNormalize {#lpnormalize}

Вычисляет единичный вектор для исходного вектора (значения кортежа — координаты вектора) в пространстве [`Lp`](https://ru.wikipedia.org/wiki/%D0%9D%D0%BE%D1%80%D0%BC%D0%B0_(%D0%BC%D0%B0%D1%82%D0%B5%D0%BC%D0%B0%D1%82%D0%B8%D0%BA%D0%B0)#%D0%9D%D0%B5%D0%BA%D0%BE%D1%82%D0%BE%D1%80%D1%8B%D0%B5_%D0%B2%D0%B8%D0%B4%D1%8B_%D0%BC%D0%B0%D1%82%D1%80%D0%B8%D1%87%D0%BD%D1%8B%D1%85_%D0%BD%D0%BE%D1%80%D0%BC).

**Синтаксис**

```sql
LpNormalize(tuple, p)
```

Синоним: `normalizeLp `.

**Аргументы**

-   `tuple` — кортеж. [Tuple](../../sql-reference/data-types/tuple.md).
-   `p` — степень. Возможные значение: любое число из промежутка [1;inf). [UInt](../../sql-reference/data-types/int-uint.md) или [Float](../../sql-reference/data-types/float.md).

**Возвращаемое значение**

-   Единичный вектор.

Тип: кортеж [Tuple](../../sql-reference/data-types/tuple.md) значений [Float](../../sql-reference/data-types/float.md).

**Пример**

Запрос:

```sql
SELECT LpNormalize((3, 4),5);
```

Результат:

```text
┌─LpNormalize((3, 4), 5)──────────────────┐
│ (0.7187302630182624,0.9583070173576831) │
└─────────────────────────────────────────┘
```

## cosineDistance {#cosinedistance}

Вычисляет косинусную разницу двух векторов (значения кортежей — координаты векторов). Чем меньше возвращаемое значение, тем больше сходство между векторами.

**Синтаксис**

```sql
cosineDistance(tuple1, tuple2)
```

**Аргументы**

-   `tuple1` — первый кортеж. [Tuple](../../sql-reference/data-types/tuple.md).
-   `tuple2` — второй кортеж. [Tuple](../../sql-reference/data-types/tuple.md).

**Возвращаемые значения**

-   Разность между единицей и косинуса угла между векторами.

Тип: [Float](../../sql-reference/data-types/float.md).

**Пример**

Запрос:

```sql
SELECT cosineDistance((1, 2), (2, 3));
```

Результат:

```text
┌─cosineDistance((1, 2), (2, 3))─┐
│           0.007722123286332261 │
└────────────────────────────────┘
```
