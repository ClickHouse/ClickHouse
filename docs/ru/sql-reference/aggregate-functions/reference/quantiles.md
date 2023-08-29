---
sidebar_position: 201
---

# Функции для нескольких квантилей {#quantiles-functions}

## quantiles {#quantiles}

Синтаксис: `quantiles(level1, level2, …)(x)`

Все функции для вычисления квантилей имеют соответствующие функции для вычисления нескольких квантилей: `quantiles`, `quantilesDeterministic`, `quantilesTiming`, `quantilesTimingWeighted`, `quantilesExact`, `quantilesExactWeighted`, `quantilesTDigest`, `quantilesBFloat16`. Эти функции вычисляют все квантили указанных уровней в один проход и возвращают массив с вычисленными значениями.

## quantilesExactExclusive {#quantilesexactexclusive}

Точно вычисляет [квантили](https://ru.wikipedia.org/wiki/Квантиль) числовой последовательности.

Чтобы получить точный результат, все переданные значения собираются в массив, который затем частично сортируется. Таким образом, функция потребляет объем памяти `O(n)`, где `n` — количество переданных значений. Для небольшого числа значений эта функция эффективна.

Эта функция эквивалентна Excel функции [PERCENTILE.EXC](https://support.microsoft.com/en-us/office/percentile-exc-function-bbaa7204-e9e1-4010-85bf-c31dc5dce4ba), [тип R6](https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample).

С наборами уровней работает эффективнее, чем [quantileExactExclusive](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexactexclusive).

**Синтаксис**

``` sql
quantilesExactExclusive(level1, level2, ...)(expr)
```

**Аргументы**

-   `expr` — выражение, зависящее от значений столбцов. Возвращает данные [числовых типов](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) или [DateTime](../../../sql-reference/data-types/datetime.md).

**Параметры**

-   `level` — уровень квантилей. Возможные значения: (0, 1) — граничные значения не учитываются. [Float](../../../sql-reference/data-types/float.md).

**Возвращаемые значения**

-   [Массив](../../../sql-reference/data-types/array.md) квантилей указанных уровней.

Тип значений массива:

-   [Float64](../../../sql-reference/data-types/float.md) для входных данных числового типа.
-   [Date](../../../sql-reference/data-types/date.md), если входные значения имеют тип `Date`.
-   [DateTime](../../../sql-reference/data-types/datetime.md), если входные значения имеют тип `DateTime`.

**Пример**

Запрос:

``` sql
CREATE TABLE num AS numbers(1000);

SELECT quantilesExactExclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x) FROM (SELECT number AS x FROM num);
```

Результат:

``` text
┌─quantilesExactExclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x)─┐
│ [249.25,499.5,749.75,899.9,949.9499999999999,989.99,998.999]        │
└─────────────────────────────────────────────────────────────────────┘
```

## quantilesExactInclusive {#quantilesexactinclusive}

Точно вычисляет [квантили](https://ru.wikipedia.org/wiki/Квантиль) числовой последовательности.

Чтобы получить точный результат, все переданные значения собираются в массив, который затем частично сортируется. Таким образом, функция потребляет объем памяти `O(n)`, где `n` — количество переданных значений. Для небольшого числа значений эта функция эффективна.

Эта функция эквивалентна Excel функции [PERCENTILE.INC](https://support.microsoft.com/en-us/office/percentile-inc-function-680f9539-45eb-410b-9a5e-c1355e5fe2ed), [тип R7](https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample).

С наборами уровней работает эффективнее, чем [quantileExactInclusive](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexactinclusive).

**Синтаксис**

``` sql
quantilesExactInclusive(level1, level2, ...)(expr)
```

**Аргументы**

-   `expr` — выражение, зависящее от значений столбцов. Возвращает данные [числовых типов](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) или [DateTime](../../../sql-reference/data-types/datetime.md).

**Параметры**

-   `level` — уровень квантилей. Возможные значения: [0, 1] — граничные значения учитываются. [Float](../../../sql-reference/data-types/float.md).

**Возвращаемые значения**

-   [Массив](../../../sql-reference/data-types/array.md) квантилей указанных уровней.

Тип значений массива:

-   [Float64](../../../sql-reference/data-types/float.md) для входных данных числового типа.
-   [Date](../../../sql-reference/data-types/date.md), если входные значения имеют тип `Date`.
-   [DateTime](../../../sql-reference/data-types/datetime.md), если входные значения имеют тип `DateTime`.

**Пример**

Запрос:

``` sql
CREATE TABLE num AS numbers(1000);

SELECT quantilesExactInclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x) FROM (SELECT number AS x FROM num);
```

Результат:

``` text
┌─quantilesExactInclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x)─┐
│ [249.75,499.5,749.25,899.1,949.05,989.01,998.001]                   │
└─────────────────────────────────────────────────────────────────────┘
```
