---
sidebar_position: 107
---

# avgWeighted {#avgweighted}

Вычисляет [среднее арифметическое взвешенное](https://ru.wikipedia.org/wiki/Среднее_арифметическое_взвешенное).

**Синтаксис**

``` sql
avgWeighted(x, weight)
```

**Аргументы**

-   `x` — значения. [Целые числа](../../../sql-reference/data-types/int-uint.md) или [числа с плавающей запятой](../../../sql-reference/data-types/float.md).
-   `weight` — веса отдельных значений. [Целые числа](../../../sql-reference/data-types/int-uint.md) или [числа с плавающей запятой](../../../sql-reference/data-types/float.md).

Типы параметров должны совпадать.

**Возвращаемое значение**

-   Среднее арифметическое взвешенное.
-   `NaN`, если все веса равны 0.

Тип: [Float64](../../../sql-reference/data-types/float.md)

**Пример**

Запрос:

``` sql
SELECT avgWeighted(x, w)
FROM values('x Int8, w Int8', (4, 1), (1, 0), (10, 2))
```

Результат:

``` text
┌─avgWeighted(x, weight)─┐
│                      8 │
└────────────────────────┘
```

