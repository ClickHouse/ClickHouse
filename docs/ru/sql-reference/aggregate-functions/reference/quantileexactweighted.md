---
toc_priority: 203
---

# quantileExactWeighted {#quantileexactweighted}

Точно вычисляет [квантиль](https://ru.wikipedia.org/wiki/Квантиль) числовой последовательности, учитывая вес каждого её элемента.

Чтобы получить точный результат, все переданные значения собираются в массив, который затем частично сортируется. Для каждого значения учитывается его вес (количество значений в выборке). В алгоритме используется хэш-таблица. Таким образом, если переданные значения часто повторяются, функция потребляет меньше оперативной памяти, чем [quantileExact](#quantileexact). Эту функцию можно использовать вместо `quantileExact` если указать вес 1.

Внутренние состояния функций `quantile*` не объединяются, если они используются в одном запросе. Если вам необходимо вычислить квантили нескольких уровней, используйте функцию [quantiles](#quantiles), это повысит эффективность запроса.

**Синтаксис**

``` sql
quantileExactWeighted(level)(expr, weight)
```

Алиас: `medianExactWeighted`.

**Аргументы**

-   `level` — уровень квантили. Опционально. Константное значение с плавающей запятой от 0 до 1. Мы рекомендуем использовать значение `level` из диапазона `[0.01, 0.99]`. Значение по умолчанию: 0.5. При `level=0.5` функция вычисляет [медиану](https://ru.wikipedia.org/wiki/Медиана_(статистика)).
-   `expr` — выражение, зависящее от значений столбцов, возвращающее данные [числовых типов](../../../sql-reference/data-types/index.md#data_types) или типов [Date](../../../sql-reference/data-types/date.md), [DateTime](../../../sql-reference/data-types/datetime.md).
-   `weight` — столбец с весам элементов последовательности. Вес — это количество повторений элемента в последовательности.

**Возвращаемое значение**

-   Quantile of the specified level.

Тип:

-   [Float64](../../../sql-reference/data-types/float.md) для входных данных числового типа.
-   [Date](../../../sql-reference/data-types/date.md), если входные значения имеют тип `Date`.
-   [DateTime](../../../sql-reference/data-types/datetime.md), если входные значения имеют тип `DateTime`.

**Пример**

Входная таблица:

``` text
┌─n─┬─val─┐
│ 0 │   3 │
│ 1 │   2 │
│ 2 │   1 │
│ 5 │   4 │
└───┴─────┘
```

Запрос:

``` sql
SELECT quantileExactWeighted(n, val) FROM t
```

Результат:

``` text
┌─quantileExactWeighted(n, val)─┐
│                             1 │
└───────────────────────────────┘
```

**Смотрите также**

-   [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
-   [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)

