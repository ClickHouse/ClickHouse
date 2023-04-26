---
sidebar_position: 301
sidebar_label: welchTTest
---

# welchTTest {#welchttest}

Вычисляет t-критерий Уэлча для выборок из двух генеральных совокупностей.

**Синтаксис**

``` sql
welchTTest(sample_data, sample_index)
```

Значения выборок берутся из столбца `sample_data`. Если  `sample_index` равно 0, то значение из этой строки принадлежит первой выборке. Во всех остальных случаях значение принадлежит второй выборке.
Проверяется нулевая гипотеза, что средние значения генеральных совокупностей совпадают. Для применения t-критерия Уэлча распределение в генеральных совокупностях должно быть нормальным. Дисперсии могут не совпадать.

**Аргументы**

-   `sample_data` — данные выборок. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) or [Decimal](../../../sql-reference/data-types/decimal.md).
-   `sample_index` — индексы выборок. [Integer](../../../sql-reference/data-types/int-uint.md).

**Возвращаемые значения**

[Кортеж](../../../sql-reference/data-types/tuple.md) с двумя элементами:

-   вычисленное значение критерия Уэлча. [Float64](../../../sql-reference/data-types/float.md).
-   вычисленное p-значение. [Float64](../../../sql-reference/data-types/float.md).


**Пример**

Таблица:

``` text
┌─sample_data─┬─sample_index─┐
│        20.3 │            0 │
│        22.1 │            0 │
│        21.9 │            0 │
│        18.9 │            1 │
│        20.3 │            1 │
│          19 │            1 │
└─────────────┴──────────────┘
```

Запрос:

``` sql
SELECT welchTTest(sample_data, sample_index) FROM welch_ttest;
```

Результат:

``` text
┌─welchTTest(sample_data, sample_index)─────┐
│ (2.7988719532211235,0.051807360348581945) │
└───────────────────────────────────────────┘
```

**Смотрите также**

-   [t-критерий Уэлча](https://ru.wikipedia.org/wiki/T-%D0%BA%D1%80%D0%B8%D1%82%D0%B5%D1%80%D0%B8%D0%B9_%D0%A3%D1%8D%D0%BB%D1%87%D0%B0)
-   [studentTTest](studentttest.md#studentttest)

