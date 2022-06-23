---
toc_priority: 68
toc_title: Функции для работы с кортежами
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
