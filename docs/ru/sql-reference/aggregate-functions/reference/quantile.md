---
sidebar_position: 200
---

# quantile {#quantile}

Приблизительно вычисляет [квантиль](https://ru.wikipedia.org/wiki/Квантиль) числовой последовательности.

Функция использует алгоритм [reservoir sampling](https://en.wikipedia.org/wiki/Reservoir_sampling) с размером резервуара до 8192 и случайным генератором чисел для для сэмплирования. Результат не детерминирован. Чтобы получить точную квантиль используйте функцию [quantileExact](#quantileexact).

Внутренние состояния функций `quantile*` не объединяются, если они используются в одном запросе. Если вам необходимо вычислить квантили нескольких уровней, используйте функцию [quantiles](#quantiles), это повысит эффективность запроса.

**Синтаксис**

``` sql
quantile(level)(expr)
```

Алиас: `median`.

**Аргументы**

-   `level` — уровень квантили. Опционально. Константное значение с плавающей запятой от 0 до 1. Мы рекомендуем использовать значение `level` из диапазона `[0.01, 0.99]`. Значение по умолчанию: 0.5. При `level=0.5` функция вычисляет [медиану](https://ru.wikipedia.org/wiki/Медиана_(статистика)).
-   `expr` — выражение, зависящее от значений столбцов, возвращающее данные [числовых типов](../../../sql-reference/data-types/index.md#data_types) или типов [Date](../../../sql-reference/data-types/date.md), [DateTime](../../../sql-reference/data-types/datetime.md).

**Возвращаемое значение**

-   Приблизительный квантиль заданного уровня.

Тип:

-   [Float64](../../../sql-reference/data-types/float.md) для входных данных числового типа.
-   [Date](../../../sql-reference/data-types/date.md), если входные значения имеют тип `Date`.
-   [DateTime](../../../sql-reference/data-types/datetime.md), если входные значения имеют тип `DateTime`.

**Пример**

Входная таблица:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

Запрос:

``` sql
SELECT quantile(val) FROM t
```

Результат:

``` text
┌─quantile(val)─┐
│           1.5 │
└───────────────┘
```

**Смотрите также**

-   [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
-   [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)

