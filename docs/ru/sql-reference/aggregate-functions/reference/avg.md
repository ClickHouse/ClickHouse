---
toc_priority: 5
---

# avg {#agg_function-avg}

Вычисляет среднее арифметическое.

**Синтаксис**

``` sql
avg(x)
```

**Параметры**

-   `x` — входное значение в формате [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) или [Decimal](../../../sql-reference/data-types/decimal.md).

**Возвращаемое значение**

-   среднее арифметическое, всегда в формате [Float64](../../../sql-reference/data-types/float.md).
-   `NaN`, если не указано входное значение `x`.

**Пример**

Запрос:

``` sql
SELECT avg(x) FROM values('x Int8', 0, 1, 2, 3, 4, 5);
```

Результат:

``` text
┌─avg(x)─┐
│    2.5 │
└────────┘
```

**Пример**

Создайте временную таблицу:

Запрос:

``` sql
CREATE table test (t UInt8) ENGINE = Memory;
```

Выполните запрос:

``` sql
SELECT avg(t) FROM test;
```

Результат:

``` text
┌─avg(x)─┐
│    nan │
└────────┘
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/aggregate-functions/reference/avg/) <!--hide-->
