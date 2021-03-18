---
toc_priority: 206
---

# quantileDeterministic {#quantiledeterministic}

Приблизительно вычисляет [квантиль](https://ru.wikipedia.org/wiki/Квантиль) числовой последовательности.

Функция использует алгоритм [reservoir sampling](https://en.wikipedia.org/wiki/Reservoir_sampling) с размером резервуара до 8192 и детерминированным алгоритмом сэмплирования. Результат детерминирован. Чтобы получить точную квантиль используйте функцию [quantileExact](#quantileexact).

Внутренние состояния функций `quantile*` не объединяются, если они используются в одном запросе. Если вам необходимо вычислить квантили нескольких уровней, используйте функцию [quantiles](#quantiles), это повысит эффективность запроса.

**Синтаксис**

``` sql
quantileDeterministic(level)(expr, determinator)
```

Алиас: `medianDeterministic`.

**Параметры**

-   `level` — Уровень квантили. Опционально. Константное значение с плавающей запятой от 0 до 1. Мы рекомендуем использовать значение `level` из диапазона `[0.01, 0.99]`. Значение по умолчанию: 0.5. При `level=0.5` функция вычисляет [медиану](https://ru.wikipedia.org/wiki/Медиана_(статистика)).
-   `expr` — Выражение над значениями столбца, которое возвращает данные [числовых типов](../../../sql-reference/data-types/index.md#data_types) или типов [Date](../../../sql-reference/data-types/date.md), [DateTime](../../../sql-reference/data-types/datetime.md).
-   `determinator` — Число, хэш которого используется при сэмплировании в алгоритме reservoir sampling, чтобы сделать результат детерминированным. В качестве детерминатора можно использовать любое определённое положительное число, например, идентификатор пользователя или события. Если одно и то же значение детерминатора попадается в выборке слишком часто, то функция выдаёт некорректный результат.

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
SELECT quantileDeterministic(val, 1) FROM t
```

Результат:

``` text
┌─quantileDeterministic(val, 1)─┐
│                           1.5 │
└───────────────────────────────┘
```

**Смотрите также**

-   [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
-   [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)

[Оригинальная статья](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/qurntiledeterministic/) <!--hide-->
