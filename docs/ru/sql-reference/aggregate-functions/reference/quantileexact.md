---
toc_priority: 202
---

# Функции quantileExact {#quantileexact-functions}

## quantileExact {#quantileexact}

Точно вычисляет [квантиль](https://ru.wikipedia.org/wiki/Квантиль) числовой последовательности.

Чтобы получить точный результат, все переданные значения собираются в массив, который затем частично сортируется. Таким образом, функция потребляет объем памяти `O(n)`, где `n` — количество переданных значений. Для небольшого числа значений эта функция эффективна.

Внутренние состояния функций `quantile*` не объединяются, если они используются в одном запросе. Если вам необходимо вычислить квантили нескольких уровней, используйте функцию [quantiles](#quantiles), это повысит эффективность запроса.

**Синтаксис**

``` sql
quantileExact(level)(expr)
```

Алиас: `medianExact`.

**Аргументы**

-   `level` — уровень квантили. Опционально. Константное значение с плавающей запятой от 0 до 1. Мы рекомендуем использовать значение `level` из диапазона `[0.01, 0.99]`. Значение по умолчанию: 0.5. При `level=0.5` функция вычисляет [медиану](https://ru.wikipedia.org/wiki/Медиана_(статистика)).
-   `expr` — выражение, зависящее от значений столбцов, возвращающее данные [числовых типов](../../../sql-reference/data-types/index.md#data_types) или типов [Date](../../../sql-reference/data-types/date.md), [DateTime](../../../sql-reference/data-types/datetime.md).

**Возвращаемое значение**

-   Квантиль заданного уровня.

Тип:

-   [Float64](../../../sql-reference/data-types/float.md) для входных данных числового типа.
-   [Date](../../../sql-reference/data-types/date.md), если входные значения имеют тип `Date`.
-   [DateTime](../../../sql-reference/data-types/datetime.md), если входные значения имеют тип `DateTime`.

**Пример**

Запрос:

``` sql
SELECT quantileExact(number) FROM numbers(10)
```

Результат:

``` text
┌─quantileExact(number)─┐
│                     5 │
└───────────────────────┘
```

## quantileExactLow {#quantileexactlow}

Как и `quantileExact`, эта функция вычисляет точный [квантиль](https://en.wikipedia.org/wiki/Quantile) числовой последовательности данных.

Чтобы получить точное значение, все переданные значения объединяются в массив, который затем полностью сортируется.  Сложность [алгоритма сортировки](https://en.cppreference.com/w/cpp/algorithm/sort) равна `O(N·log(N))`, где `N = std::distance(first, last)`.

Возвращаемое значение зависит от уровня квантили и количества элементов в выборке, то есть если уровень 0,5, то функция возвращает нижнюю медиану при чётном количестве элементов и медиану при нечётном. Медиана вычисляется аналогично реализации [median_low](https://docs.python.org/3/library/statistics.html#statistics.median_low), которая используется в python.

Для всех остальных уровней возвращается элемент с индексом, соответствующим значению `level * size_of_array`. Например:

``` sql
SELECT quantileExactLow(0.1)(number) FROM numbers(10)

┌─quantileExactLow(0.1)(number)─┐
│                             1 │
└───────────────────────────────┘
```

При использовании в запросе нескольких функций  `quantile*` с разными уровнями, внутренние состояния не объединяются (то есть запрос работает менее эффективно). В этом случае используйте функцию [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles).

**Синтаксис**

``` sql
quantileExact(level)(expr)
```

Алиас: `medianExactLow`.

**Аргументы**

-   `level` — уровень квантили. Опциональный параметр. Константное занчение с плавающей запятой от 0 до 1. Мы рекомендуем использовать значение `level` из диапазона `[0.01, 0.99]`. Значение по умолчанию: 0.5. При `level=0.5` функция вычисляет [медиану](https://en.wikipedia.org/wiki/Median).
-   `expr` — выражение, зависящее от значений столбцов, возвращающее данные [числовых типов](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) или [DateTime](../../../sql-reference/data-types/datetime.md).

**Возвращаемое значение**

-   Квантиль заданного уровня.

Тип:

-   [Float64](../../../sql-reference/data-types/float.md) для входных данных числового типа.
-   [Date](../../../sql-reference/data-types/date.md) если входные значения имеют тип `Date`.
-   [DateTime](../../../sql-reference/data-types/datetime.md) если входные значения имеют тип `DateTime`.

**Пример**

Запрос:

``` sql
SELECT quantileExactLow(number) FROM numbers(10)
```

Результат:

``` text
┌─quantileExactLow(number)─┐
│                        4 │
└──────────────────────────┘
```
## quantileExactHigh {#quantileexacthigh}

Как и `quantileExact`, эта функция вычисляет точный [квантиль](https://en.wikipedia.org/wiki/Quantile) числовой последовательности данных.

Все переданные значения объединяются в массив, который затем сортируется, чтобы получить точное значение.  Сложность [алгоритма сортировки](https://en.cppreference.com/w/cpp/algorithm/sort) равна `O(N·log(N))`, где `N = std::distance(first, last)`.

Возвращаемое значение зависит от уровня квантили и количества элементов в выборке, то есть если уровень 0,5, то функция возвращает верхнюю медиану при чётном количестве элементов и медиану при нечётном. Медиана вычисляется аналогично реализации [median_high](https://docs.python.org/3/library/statistics.html#statistics.median_high), которая используется в python. Для всех остальных уровней возвращается элемент с индексом, соответствующим значению `level * size_of_array`. 

Эта реализация ведет себя точно так же, как `quantileExact`.

При использовании в запросе нескольких функций `quantile*` с разными уровнями, внутренние состояния не объединяются (то есть запрос работает менее эффективно). В этом случае используйте функцию [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles).

**Синтаксис**

``` sql
quantileExactHigh(level)(expr)
```

Алиас: `medianExactHigh`.

**Аргументы**

-   `level` — уровень квантили. Опциональный параметр. Константное занчение с плавающей запятой от 0 до 1. Мы рекомендуем использовать значение `level` из диапазона `[0.01, 0.99]`. Значение по умолчанию: 0.5. При `level=0.5` функция вычисляет [медиану](https://en.wikipedia.org/wiki/Median).
-   `expr` — выражение, зависящее от значений столбцов, возвращающее данные [числовых типов](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) или [DateTime](../../../sql-reference/data-types/datetime.md).

**Возвращаемое значение**

-   Квантиль заданного уровня.

Тип:

-   [Float64](../../../sql-reference/data-types/float.md) для входных данных числового типа.
-   [Date](../../../sql-reference/data-types/date.md) если входные значения имеют тип `Date`.
-   [DateTime](../../../sql-reference/data-types/datetime.md) если входные значения имеют тип `DateTime`.

**Пример**

Запрос:

``` sql
SELECT quantileExactHigh(number) FROM numbers(10)
```

Результат:

``` text
┌─quantileExactHigh(number)─┐
│                         5 │
└───────────────────────────┘
```

## quantileExactExclusive {#quantileexactexclusive}

Точно вычисляет [квантиль](https://ru.wikipedia.org/wiki/Квантиль) числовой последовательности.

Чтобы получить точный результат, все переданные значения собираются в массив, который затем частично сортируется. Таким образом, функция потребляет объем памяти `O(n)`, где `n` — количество переданных значений. Для небольшого числа значений эта функция эффективна.

Эта функция эквивалентна Excel функции [PERCENTILE.EXC](https://support.microsoft.com/en-us/office/percentile-exc-function-bbaa7204-e9e1-4010-85bf-c31dc5dce4ba), [тип R6](https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample).

Если в одном запросе вызывается несколько функций `quantileExactExclusive` с разными значениями `level`, эти функции вычисляются независимо друг от друга. В таких случаях используйте функцию [quantilesExactExclusive](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantilesexactexclusive), запрос будет выполняться эффективнее.

**Синтаксис**

``` sql
quantileExactExclusive(level)(expr)
```

**Аргументы**

-   `expr` — выражение, зависящее от значений столбцов. Возвращает данные [числовых типов](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) или [DateTime](../../../sql-reference/data-types/datetime.md).

**Параметры**

-   `level` — уровень квантиля. Необязательный параметр. Возможные значения: (0, 1) — граничные значения не учитываются. Значение по умолчанию: 0.5. При `level=0.5` функция вычисляет [медиану](https://ru.wikipedia.org/wiki/Медиана_(статистика)). [Float](../../../sql-reference/data-types/float.md).

**Возвращаемое значение**

-   Квантиль заданного уровня.

Тип:

-   [Float64](../../../sql-reference/data-types/float.md) для входных данных числового типа.
-   [Date](../../../sql-reference/data-types/date.md), если входные значения имеют тип `Date`.
-   [DateTime](../../../sql-reference/data-types/datetime.md), если входные значения имеют тип `DateTime`.

**Пример**

Запрос:

``` sql
CREATE TABLE num AS numbers(1000);

SELECT quantileExactExclusive(0.6)(x) FROM (SELECT number AS x FROM num);
```

Результат:

``` text
┌─quantileExactExclusive(0.6)(x)─┐
│                          599.6 │
└────────────────────────────────┘
```

## quantileExactInclusive {#quantileexactinclusive}

Точно вычисляет [квантиль](https://ru.wikipedia.org/wiki/Квантиль) числовой последовательности.

Чтобы получить точный результат, все переданные значения собираются в массив, который затем частично сортируется. Таким образом, функция потребляет объем памяти `O(n)`, где `n` — количество переданных значений. Для небольшого числа значений эта функция эффективна.

Эта функция эквивалентна Excel функции [PERCENTILE.INC](https://support.microsoft.com/en-us/office/percentile-inc-function-680f9539-45eb-410b-9a5e-c1355e5fe2ed), [тип R7](https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample).

Если в одном запросе вызывается несколько функций `quantileExactInclusive` с разными значениями `level`, эти функции вычисляются независимо друг от друга. В таких случаях используйте функцию [quantilesExactInclusive](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantilesexactinclusive), запрос будет выполняться эффективнее.

**Синтаксис**

``` sql
quantileExactInclusive(level)(expr)
```

**Аргументы**

-   `expr` — выражение, зависящее от значений столбцов. Возвращает данные  [числовых типов](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) или [DateTime](../../../sql-reference/data-types/datetime.md).

**Параметры**

-   `level` — уровень квантиля. Необязательный параметр. Возможные значения: [0, 1] — граничные значения учитываются. Значение по умолчанию: 0.5. При `level=0.5` функция вычисляет [медиану](https://ru.wikipedia.org/wiki/Медиана_(статистика)). [Float](../../../sql-reference/data-types/float.md).

**Возвращаемое значение**

-   Квантиль заданного уровня.

Тип:

-   [Float64](../../../sql-reference/data-types/float.md) для входных данных числового типа.
-   [Date](../../../sql-reference/data-types/date.md), если входные значения имеют тип `Date`.
-   [DateTime](../../../sql-reference/data-types/datetime.md), если входные значения имеют тип `DateTime`.

**Пример**

Запрос:

``` sql
CREATE TABLE num AS numbers(1000);

SELECT quantileExactInclusive(0.6)(x) FROM (SELECT number AS x FROM num);
```

Результат:

``` text
┌─quantileExactInclusive(0.6)(x)─┐
│                          599.4 │
└────────────────────────────────┘
```

**Смотрите также**

-   [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
-   [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
