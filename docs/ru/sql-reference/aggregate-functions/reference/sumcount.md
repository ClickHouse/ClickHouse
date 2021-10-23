---
toc_priority: 144
---

# sumCount {#agg_function-sumCount}

Вычисляет сумму чисел и одновременно подсчитывает количество строк.

**Синтаксис**

``` sql
sumCount(x)
```

**Аргументы** 

-   `x` — Входное значение типа [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), или [Decimal](../../../sql-reference/data-types/decimal.md).

**Возвращаемое значение**

-   Кортеж из элементов `(sum, count)`, где `sum` — это сумма чисел и `count` — количество строк со значениями, отличными от `NULL`.

Тип: [Tuple](../../../sql-reference/data-types/tuple.md).

**Пример**

Запрос:

``` sql
CREATE TABLE s_table (x Nullable(Int8)) Engine = Log;
INSERT INTO s_table SELECT number FROM numbers(0, 20);
INSERT INTO s_table VALUES (NULL);
SELECT sumCount(x) from s_table;
```

Результат:

``` text
┌─sumCount(x)─┐
│ (190,20)    │
└─────────────┘
```

**Смотрите также**

- Настройка [optimize_fuse_sum_count_avg](../../../operations/settings/settings.md#optimize_fuse_sum_count_avg)
