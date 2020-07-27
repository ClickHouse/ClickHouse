---
toc_priority: 107
---

# avgWeighted {#avgweighted}

Вычисляет [среднее арифметическое взвешенное](https://ru.wikipedia.org/wiki/Среднее_арифметическое_взвешенное).

**Синтаксис**

``` sql
avgWeighted(x, weight)
```

**Параметры**

-   `x` — Значения. [Целые числа](../../sql-reference/aggregate-functions/reference.md) или [числа с плавающей запятой](../../sql-reference/aggregate-functions/reference.md).
-   `weight` — Веса отдельных значений. [Целые числа](../../sql-reference/aggregate-functions/reference.md) или [числа с плавающей запятой](../../sql-reference/aggregate-functions/reference.md).

Типы параметров должны совпадать.

**Возвращаемое значение**

-   Среднее арифметическое взвешенное.
-   `NaN`, если все веса равны 0.

Тип: [Float64](../../sql-reference/aggregate-functions/reference.md)

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
