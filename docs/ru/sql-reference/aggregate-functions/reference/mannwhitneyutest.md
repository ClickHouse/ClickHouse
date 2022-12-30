---
sidebar_position: 310
sidebar_label: mannWhitneyUTest
---

# mannWhitneyUTest {#mannwhitneyutest}

Вычисляет U-критерий Манна — Уитни для выборок из двух генеральных совокупностей.

**Синтаксис**

``` sql
mannWhitneyUTest[(alternative[, continuity_correction])](sample_data, sample_index)
```

Значения выборок берутся из столбца `sample_data`. Если  `sample_index` равно 0, то значение из этой строки принадлежит первой выборке. Во всех остальных случаях значение принадлежит второй выборке.
Проверяется нулевая гипотеза, что генеральные совокупности стохастически равны. Наряду с двусторонней гипотезой могут быть проверены и односторонние.
Для применения U-критерия Манна — Уитни закон распределения генеральных совокупностей не обязан быть нормальным.

**Аргументы**

-   `sample_data` — данные выборок. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) или [Decimal](../../../sql-reference/data-types/decimal.md).
-   `sample_index` — индексы выборок. [Integer](../../../sql-reference/data-types/int-uint.md).

**Параметры**

-   `alternative` — альтернативная гипотеза. (Необязательный параметр, по умолчанию: `'two-sided'`.) [String](../../../sql-reference/data-types/string.md).
    -   `'two-sided'`;
    -   `'greater'`;
    -   `'less'`.
-   `continuity_correction` — если не 0, то при вычислении p-значения применяется коррекция непрерывности. (Необязательный параметр, по умолчанию: 1.) [UInt64](../../../sql-reference/data-types/int-uint.md).

**Возвращаемые значения**

[Кортеж](../../../sql-reference/data-types/tuple.md) с двумя элементами:

-   вычисленное значение критерия Манна — Уитни. [Float64](../../../sql-reference/data-types/float.md).
-   вычисленное p-значение. [Float64](../../../sql-reference/data-types/float.md).


**Пример**

Таблица:

``` text
┌─sample_data─┬─sample_index─┐
│          10 │            0 │
│          11 │            0 │
│          12 │            0 │
│           1 │            1 │
│           2 │            1 │
│           3 │            1 │
└─────────────┴──────────────┘
```

Запрос:

``` sql
SELECT mannWhitneyUTest('greater')(sample_data, sample_index) FROM mww_ttest;
```

Результат:

``` text
┌─mannWhitneyUTest('greater')(sample_data, sample_index)─┐
│ (9,0.04042779918503192)                                │
└────────────────────────────────────────────────────────┘
```

**Смотрите также**

-   [U-критерий Манна — Уитни](https://ru.wikipedia.org/wiki/U-%D0%BA%D1%80%D0%B8%D1%82%D0%B5%D1%80%D0%B8%D0%B9_%D0%9C%D0%B0%D0%BD%D0%BD%D0%B0_%E2%80%94_%D0%A3%D0%B8%D1%82%D0%BD%D0%B8)

