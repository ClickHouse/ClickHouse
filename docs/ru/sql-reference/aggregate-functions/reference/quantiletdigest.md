---
slug: /ru/sql-reference/aggregate-functions/reference/quantiletdigest
sidebar_position: 207
---

# quantileTDigest {#quantiletdigest}

Приблизительно вычисляет [квантиль](https://ru.wikipedia.org/wiki/Квантиль) числовой последовательности, используя алгоритм [t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf).

Максимальная ошибка 1%. Потребление памяти — `log(n)`, где `n` — число значений. Результат не детерминирован и зависит от порядка выполнения запроса.

Производительность функции ниже, чем производительность функции [quantile](/docs/ru/sql-reference/aggregate-functions/reference/quantile) или [quantileTiming](/docs/ru/sql-reference/aggregate-functions/reference/quantiletiming). По соотношению размера состояния к точности вычисления, эта функция значительно превосходит `quantile`.

Внутренние состояния функций `quantile*` не объединяются, если они используются в одном запросе. Если вам необходимо вычислить квантили нескольких уровней, используйте функцию [quantiles](/docs/ru/sql-reference/aggregate-functions/reference/quantiles), это повысит эффективность запроса.

**Синтаксис**

``` sql
quantileTDigest(level)(expr)
```

Алиас: `medianTDigest`.

**Аргументы**

-   `level` — уровень квантили. Опционально. Константное значение с плавающей запятой от 0 до 1. Мы рекомендуем использовать значение `level` из диапазона `[0.01, 0.99]`. Значение по умолчанию: 0.5. При `level=0.5` функция вычисляет [медиану](https://ru.wikipedia.org/wiki/Медиана_(статистика)).
-   `expr` — выражение, зависящее от значений столбцов, возвращающее данные [числовых типов](../../../sql-reference/data-types/index.md#data_types) или типов [Date](../../../sql-reference/data-types/date.md), [DateTime](../../../sql-reference/data-types/datetime.md).

**Возвращаемое значение**

-   Приблизительную квантиль заданного уровня.

Тип:

-   [Float64](../../../sql-reference/data-types/float.md) для входных данных числового типа.
-   [Date](../../../sql-reference/data-types/date.md), если входные значения имеют тип `Date`.
-   [DateTime](../../../sql-reference/data-types/datetime.md), если входные значения имеют тип `DateTime`.

**Пример**

Запрос:

``` sql
SELECT quantileTDigest(number) FROM numbers(10)
```

Результат:

``` text
┌─quantileTDigest(number)─┐
│                     4.5 │
└─────────────────────────┘
```

**Смотрите также**

-   [median](/docs/ru/sql-reference/aggregate-functions/reference/median)
-   [quantiles](/docs/ru/sql-reference/aggregate-functions/reference/quantiles)
