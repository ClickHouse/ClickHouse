---
toc_priority: 208
---

# quantileTDigestWeighted {#quantiletdigestweighted}

Приблизительно вычисляет [квантиль](https://ru.wikipedia.org/wiki/Квантиль) числовой последовательности, используя алгоритм [t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf). Функция учитывает вес каждого элемента последовательности.

Максимальная ошибка 1%. Потребление памяти — `log(n)`, где `n` — число значений. Результат не детерминирован и зависит от порядка выполнения запроса.

Производительность функции ниже, чем производительность функции [quantile](#quantile) или [quantileTiming](#quantiletiming). По соотношению размера состояния к точности вычисления, эта функция значительно превосходит `quantile`.

Внутренние состояния функций `quantile*` не объединяются, если они используются в одном запросе. Если вам необходимо вычислить квантили нескольких уровней, используйте функцию [quantiles](#quantiles), это повысит эффективность запроса.

**Синтаксис**

``` sql
quantileTDigestWeighted(level)(expr, weight)
```

Алиас: `medianTDigest`.

**Параметры**

-   `level` — Уровень квантили. Опционально. Константное значение с плавающей запятой от 0 до 1. Мы рекомендуем использовать значение `level` из диапазона `[0.01, 0.99]`. Значение по умолчанию: 0.5. При `level=0.5` функция вычисляет [медиану](https://ru.wikipedia.org/wiki/Медиана_(статистика)).
-   `expr` — Выражение над значениями столбца, которое возвращает данные [числовых типов](../../../sql-reference/data-types/index.md#data_types) или типов [Date](../../../sql-reference/data-types/date.md), [DateTime](../../../sql-reference/data-types/datetime.md).
-   `weight` — Столбец с весам элементов последовательности. Вес — это количество повторений элемента в последовательности.

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

[Оригинальная статья](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantiledigestweighted/) <!--hide-->
