---
sidebar_position: 114
---

# groupArrayMovingAvg {#agg_function-grouparraymovingavg}

Вычисляет скользящее среднее для входных значений.

``` sql
groupArrayMovingAvg(numbers_for_summing)
groupArrayMovingAvg(window_size)(numbers_for_summing)
```

Функция может принимать размер окна в качестве параметра. Если окно не указано, то функция использует размер окна, равный количеству строк в столбце.

**Аргументы**

-   `numbers_for_summing` — [выражение](../../syntax.md#syntax-expressions), возвращающее значение числового типа.
-   `window_size` — размер окна.

**Возвращаемые значения**

-   Массив того же размера и типа, что и входные данные.

Функция использует [округление к меньшему по модулю](https://ru.wikipedia.org/wiki/Округление#Методы). Оно усекает десятичные разряды, незначимые для результирующего типа данных.

**Пример**

Таблица с исходными данными:

``` sql
CREATE TABLE t
(
    `int` UInt8,
    `float` Float32,
    `dec` Decimal32(2)
)
ENGINE = TinyLog
```

``` text
┌─int─┬─float─┬──dec─┐
│   1 │   1.1 │ 1.10 │
│   2 │   2.2 │ 2.20 │
│   4 │   4.4 │ 4.40 │
│   7 │  7.77 │ 7.77 │
└─────┴───────┴──────┘
```

Запросы:

``` sql
SELECT
    groupArrayMovingAvg(int) AS I,
    groupArrayMovingAvg(float) AS F,
    groupArrayMovingAvg(dec) AS D
FROM t
```

``` text
┌─I─────────┬─F───────────────────────────────────┬─D─────────────────────┐
│ [0,0,1,3] │ [0.275,0.82500005,1.9250001,3.8675] │ [0.27,0.82,1.92,3.86] │
└───────────┴─────────────────────────────────────┴───────────────────────┘
```

``` sql
SELECT
    groupArrayMovingAvg(2)(int) AS I,
    groupArrayMovingAvg(2)(float) AS F,
    groupArrayMovingAvg(2)(dec) AS D
FROM t
```

``` text
┌─I─────────┬─F────────────────────────────────┬─D─────────────────────┐
│ [0,1,3,5] │ [0.55,1.6500001,3.3000002,6.085] │ [0.55,1.65,3.30,6.08] │
└───────────┴──────────────────────────────────┴───────────────────────┘
```

