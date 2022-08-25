---
sidebar_position: 220
---

# simpleLinearRegression {#simplelinearregression}

Выполняет простую (одномерную) линейную регрессию.

``` sql
simpleLinearRegression(x, y)
```

Параметры:

-   `x` — столбец со значениями зависимой переменной.
-   `y` — столбец со значениями наблюдаемой переменной.

Возвращаемые значения:

Константы `(a, b)` результирующей прямой `y = a*x + b`.

**Примеры**

``` sql
SELECT arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [0, 1, 2, 3])
```

``` text
┌─arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [0, 1, 2, 3])─┐
│ (1,0)                                                             │
└───────────────────────────────────────────────────────────────────┘
```

``` sql
SELECT arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [3, 4, 5, 6])
```

``` text
┌─arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [3, 4, 5, 6])─┐
│ (1,3)                                                             │
└───────────────────────────────────────────────────────────────────┘
```

