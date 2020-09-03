---
toc_priority: 202
---

# quantileExact {#quantileexact}

Точно вычисляет [квантиль](https://ru.wikipedia.org/wiki/Квантиль) числовой последовательности.

Чтобы получить точный результат, все переданные значения собираются в массив, который затем частично сортируется. Таким образом, функция потребляет объем памяти `O(n)`, где `n` — количество переданных значений. Для небольшого числа значений эта функция эффективна.

Внутренние состояния функций `quantile*` не объединяются, если они используются в одном запросе. Если вам необходимо вычислить квантили нескольких уровней, используйте функцию [quantiles](#quantiles), это повысит эффективность запроса.

**Синтаксис**

``` sql
quantileExact(level)(expr)
```

Алиас: `medianExact`.

**Параметры**

-   `level` — Уровень квантили. Опционально. Константное значение с плавающей запятой от 0 до 1. Мы рекомендуем использовать значение `level` из диапазона `[0.01, 0.99]`. Значение по умолчанию: 0.5. При `level=0.5` функция вычисляет [медиану](https://ru.wikipedia.org/wiki/Медиана_(статистика)).
-   `expr` — Выражение над значениями столбца, которое возвращает данные [числовых типов](../../../sql-reference/data-types/index.md#data_types) или типов [Date](../../../sql-reference/data-types/date.md), [DateTime](../../../sql-reference/data-types/datetime.md).

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

**Смотрите также**

-   [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
-   [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)

[Оригинальная статья](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantileexact/) <!--hide-->
