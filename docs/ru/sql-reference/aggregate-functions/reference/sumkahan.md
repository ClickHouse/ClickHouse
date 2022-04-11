---
toc_priority: 145
---

# sumKahan {#agg_function-sumKahan}

Вычисляет сумму с использованием [компенсационного суммирования по алгоритму Кэхэна](https://ru.wikipedia.org/wiki/Алгоритм_Кэхэна).
Работает медленнее функции [sum](./sum.md).
Компенсация работает только для [Float](../../../sql-reference/data-types/float.md) типов.

**Синтаксис**

``` sql
sumKahan(x)
```

**Аргументы**

-   `x` — Входное значение типа [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), или [Decimal](../../../sql-reference/data-types/decimal.md).

**Возвращемое значение**

-  сумма чисел с типом [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), ил [Decimal](../../../sql-reference/data-types/decimal.md) зависящим от типа входящих аргументов

**Пример**

Запрос:

``` sql
SELECT sum(0.1), sumKahan(0.1) FROM numbers(10);
```

Результат:

``` text
┌───────────sum(0.1)─┬─sumKahan(0.1)─┐
│ 0.9999999999999999 │             1 │
└────────────────────┴───────────────┘
```