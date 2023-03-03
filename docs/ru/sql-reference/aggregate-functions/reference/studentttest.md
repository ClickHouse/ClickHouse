---
toc_priority: 300
toc_title: studentTTest
---

# studentTTest {#studentttest}

Вычисляет t-критерий Стьюдента для выборок из двух генеральных совокупностей.

**Синтаксис**

``` sql
studentTTest(sample_data, sample_index)
```

Значения выборок берутся из столбца `sample_data`. Если  `sample_index` равно 0, то значение из этой строки принадлежит первой выборке. Во всех остальных случаях значение принадлежит второй выборке.
Проверяется нулевая гипотеза, что средние значения генеральных совокупностей совпадают. Для применения t-критерия Стьюдента распределение в генеральных совокупностях должно быть нормальным и дисперсии должны совпадать.

**Аргументы**

-   `sample_data` — данные выборок. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) or [Decimal](../../../sql-reference/data-types/decimal.md).
-   `sample_index` — индексы выборок. [Integer](../../../sql-reference/data-types/int-uint.md).

**Возвращаемые значения**

[Кортеж](../../../sql-reference/data-types/tuple.md) с двумя элементами:

-   вычисленное значение критерия Стьюдента. [Float64](../../../sql-reference/data-types/float.md).
-   вычисленное p-значение. [Float64](../../../sql-reference/data-types/float.md).


**Пример**

Таблица:

``` text
┌─sample_data─┬─sample_index─┐
│        20.3 │            0 │
│        21.1 │            0 │
│        21.9 │            1 │
│        21.7 │            0 │
│        19.9 │            1 │
│        21.8 │            1 │
└─────────────┴──────────────┘
```

Запрос:

``` sql
SELECT studentTTest(sample_data, sample_index) FROM student_ttest;
```

Результат:

``` text
┌─studentTTest(sample_data, sample_index)───┐
│ (-0.21739130434783777,0.8385421208415731) │
└───────────────────────────────────────────┘
```

**Смотрите также**

-   [t-критерий Стьюдента](https://ru.wikipedia.org/wiki/T-%D0%BA%D1%80%D0%B8%D1%82%D0%B5%D1%80%D0%B8%D0%B9_%D0%A1%D1%82%D1%8C%D1%8E%D0%B4%D0%B5%D0%BD%D1%82%D0%B0)
-   [welchTTest](welchttest.md#welchttest)

