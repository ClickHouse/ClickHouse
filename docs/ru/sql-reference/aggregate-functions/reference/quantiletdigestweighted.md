---
sidebar_position: 208
---

# quantileTDigestWeighted {#quantiletdigestweighted}

Приблизительно вычисляет [квантиль](https://ru.wikipedia.org/wiki/Квантиль) числовой последовательности, используя алгоритм [t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf). Функция учитывает вес каждого элемента последовательности.

Максимальная ошибка 1%. Потребление памяти — `log(n)`, где `n` — число значений. Результат не детерминирован и зависит от порядка выполнения запроса.

Производительность функции ниже, чем производительность функции [quantile](#quantile) или [quantileTiming](#quantiletiming). По соотношению размера состояния к точности вычисления, эта функция значительно превосходит `quantile`.

Внутренние состояния функций `quantile*` не объединяются, если они используются в одном запросе. Если вам необходимо вычислить квантили нескольких уровней, используйте функцию [quantiles](#quantiles), это повысит эффективность запроса.

    :::note "Примечание"
    Использование `quantileTDigestWeighted` [не рекомендуется для небольших наборов данных](https://github.com/tdunning/t-digest/issues/167#issuecomment-828650275) и может привести к значительной ошибке. Рассмотрите возможность использования [`quantileTDigest`](../../../sql-reference/aggregate-functions/reference/quantiletdigest.md) в таких случаях.
    :::
**Синтаксис**

``` sql
quantileTDigestWeighted(level)(expr, weight)
```

Синоним: `medianTDigestWeighted`.

**Аргументы**

-   `level` — уровень квантили. Опционально. Константное значение с плавающей запятой от 0 до 1. Мы рекомендуем использовать значение `level` из диапазона `[0.01, 0.99]`. Значение по умолчанию: 0.5. При `level=0.5` функция вычисляет [медиану](https://ru.wikipedia.org/wiki/Медиана_(статистика)).
-   `expr` — выражение, зависящее от значений столбцов, возвращающее данные [числовых типов](../../../sql-reference/data-types/index.md#data_types) или типов [Date](../../../sql-reference/data-types/date.md), [DateTime](../../../sql-reference/data-types/datetime.md).
-   `weight` — столбец с весам элементов последовательности. Вес — это количество повторений элемента в последовательности.

**Возвращаемое значение**

-   Приблизительный квантиль заданного уровня.

Тип:

-   [Float64](../../../sql-reference/data-types/float.md) для входных данных числового типа.
-   [Date](../../../sql-reference/data-types/date.md), если входные значения имеют тип `Date`.
-   [DateTime](../../../sql-reference/data-types/datetime.md), если входные значения имеют тип `DateTime`.

**Пример**

Запрос:

``` sql
SELECT quantileTDigestWeighted(number, 1) FROM numbers(10)
```

Результат:

``` text
┌─quantileTDigestWeighted(number, 1)─┐
│                                4.5 │
└────────────────────────────────────┘
```

**Смотрите также**

-   [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
-   [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)

