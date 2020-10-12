---
toc_priority: 109
---

# topKWeighted {#topkweighted}

Аналогична `topK`, но дополнительно принимает положительный целочисленный параметр `weight`. Каждое значение учитывается `weight` раз при расчёте частоты.

**Синтаксис**

``` sql
topKWeighted(N)(x, weight)
```

**Параметры**

-   `N` — Количество элементов для выдачи.

**Аргументы**

-   `x` – значение.
-   `weight` — вес. [UInt8](../../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

Возвращает массив значений с максимально приближенной суммой весов.

**Пример**

Запрос:

``` sql
SELECT topKWeighted(10)(number, number) FROM numbers(1000)
```

Результат:

``` text
┌─topKWeighted(10)(number, number)──────────┐
│ [999,998,997,996,995,994,993,992,991,990] │
└───────────────────────────────────────────┘
```

[Оригинальная статья](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/topkweighted/) <!--hide-->
